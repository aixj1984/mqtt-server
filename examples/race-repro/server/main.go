// SPDX-License-Identifier: MIT
//
// race-repro broker: intentionally tight limits to surface remaining concurrency bugs.
//
// Run:
//
//	go run ./examples/race-repro/server -addr :1883 -http :18080
//
// Stats:
//
//	curl http://127.0.0.1:18080/stats
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/aixj1984/mqtt-server"
	"github.com/aixj1984/mqtt-server/hooks/auth"
	"github.com/aixj1984/mqtt-server/listeners"
)

func main() {
	addr := flag.String("addr", ":1883", "MQTT TCP listen address")
	httpAddr := flag.String("http", ":18080", "HTTP stats listen address")
	maxInflight := flag.Uint("max-inflight", 64, "MaximumInflight (keep small to stress Len-then-Reserve TOCTOU)")
	receiveMaximum := flag.Uint("receive-maximum", 8, "server Capability ReceiveMaximum / default send window")
	msgExpiry := flag.Int64("message-expiry", 5, "MaximumMessageExpiryInterval seconds (stress ClearExpired without quota restore)")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	server := mqtt.New(&mqtt.Options{
		Capabilities: &mqtt.Capabilities{
			ReceiveMaximum:               uint16(*receiveMaximum),
			MaximumInflight:              uint16(*maxInflight),
			MaximumMessageExpiryInterval: *msgExpiry,
			MaximumClientWritesPending:   256,
			MaximumSessionExpiryInterval: 300,
			MaximumClients:               10000,
			MaximumQos:                   2,
			SharedSubAvailable:           1,
			WildcardSubAvailable:         1,
			RetainAvailable:              1,
			MinimumProtocolVersion:       3,
		},
		ClientNetWriteBufferSize: 4096,
		ClientNetReadBufferSize:  4096,
		SysTopicResendInterval:   30,
	})

	level := new(slog.LevelVar)
	server.Log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	level.Set(slog.LevelInfo)

	_ = server.AddHook(new(auth.AllowHook), nil)

	tcp := listeners.NewTCP(listeners.Config{ID: "race-repro", Address: *addr})
	if err := server.AddListener(tcp); err != nil {
		log.Fatal(err)
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(collectStats(server))
		})
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		})
		log.Printf("stats http on %s (/stats)", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, mux); err != nil {
			log.Printf("http stats stopped: %v", err)
		}
	}()

	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			s := collectStats(server)
			log.Printf("broker messages_received=%d messages_sent=%d inflight=%d inflight_dropped=%d messages_dropped=%d clients=%d anomalies=%d",
				s.MessagesReceived, s.MessagesSent, s.Inflight, s.InflightDropped, s.MessagesDropped, s.ClientsConnected, s.AnomalyCount)
			for _, c := range s.Clients {
				if c.Anomaly {
					log.Printf("  ANOMALY client=%s inflight=%d deferred=%d reserved=%d sendQuota=%d/%d",
						c.ID, c.InflightLen, c.Deferred, c.Reserved, c.SendQuota, c.MaxSendQuota)
				}
			}
		}
	}()

	go func() {
		if err := server.Serve(); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("race-repro broker mqtt=%s maxInflight=%d receiveMaximum=%d messageExpiry=%ds",
		*addr, *maxInflight, *receiveMaximum, *msgExpiry)
	log.Printf("scenarios: fanin | packetid-clash | session-takeover | expiry  (see client -scenario)")

	<-sigs
	server.Close()
}

type clientStat struct {
	ID           string `json:"id"`
	InflightLen  int    `json:"inflight_len"`
	Deferred     int    `json:"deferred"`
	Reserved     int    `json:"reserved"`
	SendQuota    int32  `json:"send_quota"`
	MaxSendQuota int32  `json:"max_send_quota"`
	RecvQuota    int32  `json:"receive_quota"`
	MaxRecvQuota int32  `json:"max_receive_quota"`
	Anomaly      bool   `json:"anomaly"`
	Note         string `json:"note,omitempty"`
}

type brokerStats struct {
	Time              string       `json:"time"`
	MessagesReceived  int64        `json:"messages_received"`
	MessagesSent      int64        `json:"messages_sent"`
	Inflight          int64        `json:"inflight"`
	InflightDropped   int64        `json:"inflight_dropped"`
	MessagesDropped   int64        `json:"messages_dropped"`
	ClientsConnected  int64        `json:"clients_connected"`
	AnomalyCount      int          `json:"anomaly_count"`
	Clients           []clientStat `json:"clients"`
	Hints             []string     `json:"hints"`
}

func collectStats(server *mqtt.Server) brokerStats {
	out := brokerStats{
		Time:             time.Now().Format(time.RFC3339),
		MessagesReceived: atomic.LoadInt64(&server.Info.MessagesReceived),
		MessagesSent:     atomic.LoadInt64(&server.Info.MessagesSent),
		Inflight:         atomic.LoadInt64(&server.Info.Inflight),
		InflightDropped:  atomic.LoadInt64(&server.Info.InflightDropped),
		MessagesDropped:  atomic.LoadInt64(&server.Info.MessagesDropped),
		ClientsConnected: atomic.LoadInt64(&server.Info.ClientsConnected),
		Hints: []string{
			"fanin: inflight_len > max_inflight capability, or inflight_dropped rising under concurrent publishers",
			"packetid-clash: outbound deferred/reserved vanish while subscriber also publishes on same client",
			"expiry: after message-expiry window, send_quota stuck low while deferred cleared without restore",
			"session-takeover: after reconnect same client id, send_quota jumps to max while inflight_len > 0",
		},
	}

	for _, cl := range server.Clients.GetAll() {
		length, deferred, reserved, sendQ, maxSend, recvQ, maxRecv := cl.State.Inflight.QuotaStatsDetailed()
		cs := clientStat{
			ID:           cl.ID,
			InflightLen:  length,
			Deferred:     deferred,
			Reserved:     reserved,
			SendQuota:    sendQ,
			MaxSendQuota: maxSend,
			RecvQuota:    recvQ,
			MaxRecvQuota: maxRecv,
		}

		// Heuristics for real accounting bugs (not deferred backlogs with free sendQuota).
		capMax := int(server.Options.Capabilities.MaximumInflight)
		sentWaiting := length - deferred - reserved
		switch {
		case length > capMax:
			cs.Anomaly = true
			cs.Note = fmt.Sprintf("inflight_len %d exceeds MaximumInflight %d (Len-then-Reserve TOCTOU)", length, capMax)
		case length == 0 && maxSend > 0 && sendQ == 0:
			cs.Anomaly = true
			cs.Note = "empty inflight but send_quota=0 (quota leak)"
		case length > 0 && maxSend > 0 && sendQ == 0 && deferred == length:
			cs.Anomaly = true
			cs.Note = "all deferred + send_quota=0 (starved window / leak)"
		case sentWaiting > 0 && maxSend > 0 && sendQ == maxSend:
			// Truly inconsistent: messages awaiting ACK should have consumed sendQuota.
			cs.Anomaly = true
			cs.Note = fmt.Sprintf("sent_waiting=%d but send_quota at maximum (session clone / ResetSendQuota race)", sentWaiting)
		case deferred > 0 && sendQ > 0:
			// Informational only — broker should flush on next tick / PUBACK.
			cs.Note = fmt.Sprintf("deferred_backlog=%d with send_quota=%d (flush pending)", deferred, sendQ)
		}

		if cs.Anomaly {
			out.AnomalyCount++
		}
		out.Clients = append(out.Clients, cs)
	}
	return out
}
