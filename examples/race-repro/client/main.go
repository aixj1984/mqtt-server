// SPDX-License-Identifier: MIT
//
// race-repro client: stress the broker to surface remaining concurrency / accounting bugs.
//
// Prerequisites: start the demo broker first
//
//	go run ./examples/race-repro/server -addr :1883 -http :18080 \
//	  -max-inflight 64 -receive-maximum 8 -message-expiry 5
//
// Scenarios:
//
//	fanin            many publishers → one slow shared-subscription consumer
//	                 (MaximumInflight Len-then-Reserve TOCTOU / fan-in pressure)
//	packetid-clash   subscriber also publishes on the same connection
//	                 (inbound PacketID may Delete outbound Inflight entry)
//	session-takeover reconnect with CleanStart=false while traffic continues
//	                 (Clone + ResetSendQuota while inflight remains)
//	expiry           park deferred msgs then wait past MaximumMessageExpiryInterval
//	                 (ClearExpiredInflights without IncreaseSendQuota)
//	all              run fanin then print summary (default for quick smoke)
//
// Verdict (see printVerdict): REPRODUCED only on broker anomaly_count>0 or ACK stall.
// Drop counters are compared as per-scenario deltas (broker totals accumulate across runs).
// received=0 is inconclusive setup, not "rate-limited".
//
// Example:
//
//	go run ./examples/race-repro/client -scenario fanin -publishers 20 -rate 800 -duration 45s -slow-ms 20
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "broker URL")
	statsURL := flag.String("stats", "http://127.0.0.1:18080/stats", "broker /stats URL")
	scenario := flag.String("scenario", "fanin", "fanin|packetid-clash|session-takeover|expiry|all")
	publishers := flag.Int("publishers", 20, "concurrent publisher count")
	rate := flag.Int("rate", 500, "aggregate publish rate (msg/s) across publishers")
	duration := flag.Duration("duration", 30*time.Second, "scenario duration")
	slowMs := flag.Int("slow-ms", 10, "subscriber handler sleep before ACK (ms); 0 = immediate ACK")
	receiveMax := flag.Uint("receive-maximum", 8, "MQTT5 client ReceiveMaximum for subscriber")
	topic := flag.String("topic", "race/demo/data", "publish/subscribe topic (non-share)")
	shareTopic := flag.String("share-topic", "$share/race-group/race/demo/data", "shared subscription filter")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("scenario=%s broker=%s publishers=%d rate=%d duration=%s slow-ms=%d",
		*scenario, *broker, *publishers, *rate, *duration, *slowMs)

	switch *scenario {
	case "fanin":
		runFanIn(ctx, *broker, *statsURL, *shareTopic, *topic, *publishers, *rate, *duration, *slowMs, uint16(*receiveMax))
	case "packetid-clash":
		runPacketIDClash(ctx, *broker, *statsURL, *topic, *publishers, *rate, *duration, *slowMs, uint16(*receiveMax))
	case "session-takeover":
		runSessionTakeover(ctx, *broker, *statsURL, *shareTopic, *topic, *publishers, *rate, *duration, uint16(*receiveMax))
	case "expiry":
		runExpiry(ctx, *broker, *statsURL, *shareTopic, *topic, *publishers, *rate, *duration, uint16(*receiveMax))
	case "all":
		runFanIn(ctx, *broker, *statsURL, *shareTopic, *topic, *publishers, *rate, *duration, *slowMs, uint16(*receiveMax))
		log.Printf("--- next: packetid-clash ---")
		runPacketIDClash(ctx, *broker, *statsURL, *topic, *publishers, *rate, *duration/2, *slowMs, uint16(*receiveMax))
	default:
		log.Fatalf("unknown scenario %q", *scenario)
	}
}

// ─── scenarios ───────────────────────────────────────────────────────────────

func runFanIn(ctx context.Context, broker, statsURL, shareTopic, pubTopic string, pubs, rate int, dur time.Duration, slowMs int, recvMax uint16) {
	log.Printf("[fanin] target: MaximumInflight Len-then-Reserve TOCTOU + silent pressure on one subscriber")
	before := fetchStats(statsURL)

	var published, received, acked atomic.Int64
	subDone := make(chan struct{})
	sub, err := startSubscriber(ctx, broker, "race-sub-fanin", shareTopic, recvMax, slowMs, &received, &acked, subDone)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Disconnect(ctx)

	stopPubs := startPublishers(ctx, broker, pubTopic, pubs, rate, &published)
	defer stopPubs()

	waitOr(ctx, dur)
	stopPubs()
	time.Sleep(3 * time.Second) // observe window

	printVerdict("fanin", published.Load(), received.Load(), acked.Load(), before, fetchStats(statsURL))
	close(subDone)
}

func runPacketIDClash(ctx context.Context, broker, statsURL, topic string, pubs, rate int, dur time.Duration, slowMs int, recvMax uint16) {
	log.Printf("[packetid-clash] target: inbound PUBLISH PacketID deletes outbound Inflight on same client")
	before := fetchStats(statsURL)

	var published, received, selfPub, acked atomic.Int64

	// Victim: receives fan-in AND publishes on the same connection (packet IDs from both directions).
	u, err := url.Parse(broker)
	if err != nil {
		log.Fatal(err)
	}
	cliCfg := baseClientConfig(u, "race-sub-clash", recvMax, false, 120)
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer cm.Disconnect(ctx)
	if err := cm.AwaitConnection(ctx); err != nil {
		log.Fatal(err)
	}

	cm.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		received.Add(1)
		if slowMs > 0 {
			time.Sleep(time.Duration(slowMs) * time.Millisecond)
		}
		acked.Add(1)
		return true, nil
	})
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}},
	}); err != nil {
		log.Fatal(err)
	}

	// Self-publisher on the same connection (generates client→broker PacketIDs).
	selfCtx, selfCancel := context.WithCancel(ctx)
	defer selfCancel()
	go func() {
		t := time.NewTicker(time.Second / 200) // ~200 msg/s self publish
		defer t.Stop()
		n := 0
		for {
			select {
			case <-selfCtx.Done():
				return
			case <-t.C:
				n++
				_, err := cm.Publish(selfCtx, &paho.Publish{
					Topic:   topic,
					QoS:     1,
					Payload: []byte(fmt.Sprintf(`{"src":"self","n":%d,"ts":%d}`, n, time.Now().UnixMilli())),
				})
				if err == nil {
					selfPub.Add(1)
				}
			}
		}
	}()

	stopPubs := startPublishers(ctx, broker, topic, pubs, rate, &published)
	defer stopPubs()

	waitOr(ctx, dur)
	selfCancel()
	stopPubs()
	time.Sleep(3 * time.Second)

	log.Printf("[packetid-clash] external_published=%d self_published=%d received=%d acked=%d",
		published.Load(), selfPub.Load(), received.Load(), acked.Load())
	// received should approach external+self if no clash loss; gap indicates silent drop.
	expected := published.Load() + selfPub.Load()
	printVerdict("packetid-clash", expected, received.Load(), acked.Load(), before, fetchStats(statsURL))
}

func runSessionTakeover(ctx context.Context, broker, statsURL, shareTopic, pubTopic string, pubs, rate int, dur time.Duration, recvMax uint16) {
	log.Printf("[session-takeover] target: Clone inflight then ResetSendQuota to full while messages remain")
	before := fetchStats(statsURL)

	var published, received, acked atomic.Int64
	u, err := url.Parse(broker)
	if err != nil {
		log.Fatal(err)
	}

	// First session: persistent, subscribe, start receiving.
	cfg1 := baseClientConfig(u, "race-session-victim", recvMax, false, 300)
	cm1, err := autopaho.NewConnection(ctx, cfg1)
	if err != nil {
		log.Fatal(err)
	}
	cm1.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		received.Add(1)
		time.Sleep(30 * time.Millisecond) // stay behind so inflight builds
		acked.Add(1)
		return true, nil
	})
	if err := cm1.AwaitConnection(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := cm1.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: shareTopic, QoS: 1}},
	}); err != nil {
		log.Fatal(err)
	}

	stopPubs := startPublishers(ctx, broker, pubTopic, pubs, rate, &published)
	time.Sleep(2 * time.Second) // build inflight
	log.Printf("[session-takeover] before takeover published=%d received=%d — check /stats for victim send_quota",
		published.Load(), received.Load())
	dumpStats(statsURL)

	// Take over same ClientID from another connection.
	log.Printf("[session-takeover] reconnecting same client id (session takeover)...")
	_ = cm1.Disconnect(ctx)

	cfg2 := baseClientConfig(u, "race-session-victim", recvMax, false, 300)
	cm2, err := autopaho.NewConnection(ctx, cfg2)
	if err != nil {
		log.Fatal(err)
	}
	defer cm2.Disconnect(ctx)
	cm2.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		received.Add(1)
		acked.Add(1)
		return true, nil
	})
	if err := cm2.AwaitConnection(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := cm2.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: shareTopic, QoS: 1}},
	}); err != nil {
		log.Fatal(err)
	}

	log.Printf("[session-takeover] after takeover — look for anomaly: inflight_len>0 && send_quota==max")
	dumpStats(statsURL)

	waitOr(ctx, dur)
	stopPubs()
	time.Sleep(2 * time.Second)
	printVerdict("session-takeover", published.Load(), received.Load(), acked.Load(), before, fetchStats(statsURL))
}

func runExpiry(ctx context.Context, broker, statsURL, shareTopic, pubTopic string, pubs, rate int, dur time.Duration, recvMax uint16) {
	log.Printf("[expiry] target: ClearExpiredInflights deletes without IncreaseSendQuota (server -message-expiry must be small)")
	before := fetchStats(statsURL)

	var published, received, acked atomic.Int64

	// Very slow / paused subscriber so messages park as deferred and sit until expiry sweeper runs.
	u, err := url.Parse(broker)
	if err != nil {
		log.Fatal(err)
	}
	cfg := baseClientConfig(u, "race-sub-expiry", 1, true, 0) // ReceiveMaximum=1 → park quickly
	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer cm.Disconnect(ctx)

	pauseACK := atomic.Bool{}
	pauseACK.Store(true)
	cm.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		received.Add(1)
		for pauseACK.Load() {
			select {
			case <-ctx.Done():
				return true, nil
			case <-time.After(200 * time.Millisecond):
			}
		}
		acked.Add(1)
		return true, nil
	})
	if err := cm.AwaitConnection(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: shareTopic, QoS: 1}},
	}); err != nil {
		log.Fatal(err)
	}

	stopPubs := startPublishers(ctx, broker, pubTopic, pubs, rate, &published)
	time.Sleep(3 * time.Second)
	stopPubs()
	log.Printf("[expiry] traffic stopped; holding ACKs so deferred/inflight age out (wait ~ message-expiry + 3s)")
	dumpStats(statsURL)

	waitOr(ctx, dur)
	log.Printf("[expiry] after wait — look for send_quota under-count / all-deferred stall")
	dumpStats(statsURL)

	pauseACK.Store(false)
	time.Sleep(5 * time.Second)
	printVerdict("expiry", published.Load(), received.Load(), acked.Load(), before, fetchStats(statsURL))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func baseClientConfig(u *url.URL, clientID string, recvMax uint16, clean bool, sessionExpiry uint32) autopaho.ClientConfig {
	return autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: clean,
		SessionExpiryInterval:         sessionExpiry,
		ConnectPacketBuilder: func(cp *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			if cp.Properties == nil {
				cp.Properties = &paho.ConnectProperties{}
			}
			cp.Properties.ReceiveMaximum = paho.Uint16(recvMax)
			return cp, nil
		},
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnClientError: func(err error) {
				log.Printf("client %s error: %v", clientID, err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Printf("client %s server disconnect: %+v", clientID, d)
			},
		},
	}
}

func startSubscriber(ctx context.Context, broker, clientID, topic string, recvMax uint16, slowMs int, received, acked *atomic.Int64, _ chan struct{}) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(broker)
	if err != nil {
		return nil, err
	}
	cfg := baseClientConfig(u, clientID, recvMax, true, 0)
	cfg.OnConnectionUp = func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
		if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}},
		}); err != nil {
			log.Printf("subscribe %s: %v", topic, err)
		}
	}
	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		return nil, err
	}
	cm.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		received.Add(1)
		if slowMs > 0 {
			time.Sleep(time.Duration(slowMs) * time.Millisecond)
		}
		acked.Add(1)
		return true, nil
	})
	if err := cm.AwaitConnection(ctx); err != nil {
		return nil, err
	}
	return cm, nil
}

func startPublishers(ctx context.Context, broker, topic string, pubs, rate int, published *atomic.Int64) (stop func()) {
	pubCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	if pubs < 1 {
		pubs = 1
	}
	per := rate / pubs
	if per < 1 {
		per = 1
	}
	interval := time.Second / time.Duration(per)

	for i := 0; i < pubs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			u, err := url.Parse(broker)
			if err != nil {
				log.Printf("publisher %d url: %v", id, err)
				return
			}
			cfg := baseClientConfig(u, fmt.Sprintf("race-pub-%d", id), 100, true, 0)
			cm, err := autopaho.NewConnection(pubCtx, cfg)
			if err != nil {
				log.Printf("publisher %d connect: %v", id, err)
				return
			}
			defer cm.Disconnect(context.Background())
			if err := cm.AwaitConnection(pubCtx); err != nil {
				return
			}
			t := time.NewTicker(interval)
			defer t.Stop()
			n := 0
			for {
				select {
				case <-pubCtx.Done():
					return
				case <-t.C:
					n++
					_, err := cm.Publish(pubCtx, &paho.Publish{
						Topic:   topic,
						QoS:     1,
						Payload: []byte(fmt.Sprintf(`{"pub":%d,"n":%d,"ts":%d}`, id, n, time.Now().UnixMilli())),
					})
					if err == nil {
						published.Add(1)
					}
				}
			}
		}(i)
	}

	return func() {
		cancel()
		wg.Wait()
	}
}

func waitOr(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func fetchStats(statsURL string) map[string]any {
	resp, err := http.Get(statsURL)
	if err != nil {
		log.Printf("stats fetch: %v", err)
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		log.Printf("stats json: %v", err)
		return nil
	}
	return m
}

func dumpStats(statsURL string) {
	m := fetchStats(statsURL)
	if m == nil {
		return
	}
	b, _ := json.MarshalIndent(m, "", "  ")
	log.Printf("broker /stats:\n%s", string(b))
}

func printVerdict(scenario string, published, received, acked int64, before, after map[string]any) {
	loss := published - received
	if loss < 0 {
		loss = 0
	}
	pct := 0.0
	if published > 0 {
		pct = float64(received) / float64(published) * 100
	}
	log.Printf("══════ %s RESULT ══════", scenario)
	log.Printf("published=%d received=%d acked=%d gap=%d recv_ratio=%.2f%%",
		published, received, acked, loss, pct)

	// Drop counters on the broker are cumulative across scenarios — only the delta
	// for this run may explain a client gap. anomaly_count is point-in-time.
	anomalies := anomalyCount(after)
	inflightDropped := statsInt64(after, "inflight_dropped") - statsInt64(before, "inflight_dropped")
	messagesDropped := statsInt64(after, "messages_dropped") - statsInt64(before, "messages_dropped")
	if inflightDropped < 0 {
		inflightDropped = 0
	}
	if messagesDropped < 0 {
		messagesDropped = 0
	}
	brokerDrops := inflightDropped + messagesDropped
	if after != nil {
		log.Printf("broker(total) messages_received=%v messages_sent=%v inflight=%v inflight_dropped=%v messages_dropped=%v anomaly_count=%v",
			after["messages_received"], after["messages_sent"], after["inflight"],
			after["inflight_dropped"], after["messages_dropped"], anomalies)
		log.Printf("broker(delta) inflight_dropped=%d messages_dropped=%d", inflightDropped, messagesDropped)
		if clients, ok := after["clients"].([]any); ok {
			for _, c := range clients {
				cm, _ := c.(map[string]any)
				if cm["anomaly"] == true {
					log.Printf("  ANOMALY %+v", cm)
				}
			}
		}
	}

	// Verdict priority:
	//  1) broker anomaly heuristics (quota leak / inflight accounting bugs)
	//  2) subscriber stall (received but not completing ACKs)
	//  3) received=0 while publishing → setup/routing failed (not a race signal)
	//  4) gap explained by THIS RUN's MaximumInflight / outbound drops → NOT a race repro
	//  5) unexplained gap → inconclusive
	switch {
	case anomalies > 0:
		log.Printf("VERDICT: REPRODUCED %s — broker anomaly_count=%d (inspect ANOMALY lines)", scenario, anomalies)
	case received > 0 && acked*2 < received:
		log.Printf("VERDICT: REPRODUCED %s — subscriber stall (received=%d acked=%d)", scenario, received, acked)
	case published > 0 && received == 0:
		log.Printf("VERDICT: inconclusive %s — published=%d but received=0 (subscriber never got traffic; check share-group leftovers / topic / restart broker)",
			scenario, published)
	case loss > 0 && pct < 95 && brokerDrops >= loss/2:
		log.Printf("VERDICT: NOT reproduced — gap=%d mostly rate-limited drops this run (Δinflight_dropped=%d Δmessages_dropped=%d); anomaly_count=0",
			loss, inflightDropped, messagesDropped)
	case loss > 0 && pct < 95 && after == nil:
		log.Printf("VERDICT: inconclusive %s — client gap without /stats (start race-repro server with -http)", scenario)
	case loss > 0 && pct < 95:
		log.Printf("VERDICT: inconclusive %s — gap=%d not explained by this-run drops (Δdropped=%d) or anomalies; retry or inspect /stats",
			scenario, loss, brokerDrops)
	default:
		log.Printf("VERDICT: no clear bug this run (races are probabilistic — retry with higher -rate/-publishers)")
	}
}

func anomalyCount(stats map[string]any) int {
	return int(statsInt64(stats, "anomaly_count"))
}

func statsInt64(stats map[string]any, key string) int64 {
	if stats == nil {
		return 0
	}
	switch v := stats[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	default:
		return 0
	}
}
