// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt contributors

package mqtt

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aixj1984/mqtt-server/packets"
	"github.com/stretchr/testify/require"
)

// TestConcurrentPublishPacketIDNoSilentOverwrite hammers publishToClient from many
// goroutines against one MQTT5 subscriber. Previously NextPacketID only checked
// Inflight.Get under cl.Lock, then Set ran later without holding that lock — two
// publishers could receive the same PacketID; the second Set silently overwrote the
// first and the overwritten message vanished with no drop counter.
//
// Expectation after the Reserve fix: every successful publish is either still in
// Inflight or can be drained via PUBACK; published == acked, InflightEmpty, no
// "collision" drops under normal load.
func TestConcurrentPublishPacketIDNoSilentOverwrite(t *testing.T) {
	const (
		workers        = 32
		msgsPerWorker  = 100
		receiveMaximum = 50
		totalMsgs      = workers * msgsPerWorker
	)

	s := newServer()
	s.Options.Capabilities.MaximumInflight = 65535
	cl, r, w := newMQTT5QuotaTestClient(receiveMaximum, 65535)
	s.Clients.Add(cl)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, r)
	}()
	defer func() {
		_ = w.Close()
		<-done
	}()

	sub := packets.Subscription{Filter: "race/topic", Qos: 1}
	var published atomic.Int64
	var publishErrs atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)
	start := make(chan struct{})
	for wkr := 0; wkr < workers; wkr++ {
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < msgsPerWorker; i++ {
				pk := packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
					TopicName:   "race/topic",
					Payload:     []byte(fmt.Sprintf("w%d-m%d", worker, i)),
					Created:     time.Now().Unix(),
				}
				_, err := s.publishToClient(cl, sub, pk)
				if err != nil {
					publishErrs.Add(1)
					continue
				}
				published.Add(1)
			}
		}(wkr)
	}
	close(start)
	wg.Wait()

	require.Equal(t, int64(0), publishErrs.Load(), "publishToClient must not fail under this load")
	require.Equal(t, int64(totalMsgs), published.Load())
	require.Equal(t, totalMsgs, cl.State.Inflight.Len(),
		"every publish must still be tracked (sent or deferred); silent overwrite would shrink Inflight")

	// Drain via PUBACK until empty — proves every stored message is still reachable.
	deadline := time.Now().Add(30 * time.Second)
	acked := 0
	for cl.State.Inflight.Len() > 0 {
		require.True(t, time.Now().Before(deadline),
			"drain stalled: inflight=%d sendQuota=%d", cl.State.Inflight.Len(),
			atomic.LoadInt32(&cl.State.Inflight.sendQuota))

		id, ok := firstAckablePacketID(cl)
		if !ok {
			// Only deferred left — poke resume with Pingreq (also exercises safety valve).
			require.NoError(t, s.processPacket(cl, packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
			}))
			id, ok = firstAckablePacketID(cl)
			require.True(t, ok, "unable to resume deferred publishes")
		}
		require.NoError(t, s.processPacket(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    id,
		}))
		acked++
	}

	require.Equal(t, totalMsgs, acked)
	require.Equal(t, int32(receiveMaximum), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.InflightDropped))
}

// TestPacketIDReservePreventsTOCTOU is a focused unit test: without Reserve, two
// callers can observe the same free id; with Reserve the second allocation skips it.
func TestPacketIDReservePreventsTOCTOU(t *testing.T) {
	cl, _, _ := newTestClient()

	id1, err := cl.NextPacketID()
	require.NoError(t, err)

	// Before Set/Replace, the id must already be occupied so a peer NextPacketID
	// cannot reuse it — that is the TOCTOU fix.
	_, occupied := cl.State.Inflight.Get(uint16(id1))
	require.True(t, occupied)
	pk, ok := cl.State.Inflight.Get(uint16(id1))
	require.True(t, ok)
	require.Equal(t, InflightExpiryReserved, pk.Expiry)

	id2, err := cl.NextPacketID()
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	require.True(t, cl.State.Inflight.Replace(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    uint16(id1),
		TopicName:   "t",
		Payload:     []byte("a"),
	}))
	require.False(t, cl.State.Inflight.Replace(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    uint16(id1),
		TopicName:   "t",
		Payload:     []byte("overwrite-real"),
	}), "replacing a live QoS entry must be rejected")
}
