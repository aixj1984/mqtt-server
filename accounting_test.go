// SPDX-License-Identifier: MIT

package mqtt

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aixj1984/mqtt-server/packets"
	"github.com/stretchr/testify/require"
)

// TestMaximumInflightReserveAtomic: concurrent publishers cannot exceed MaximumInflight
// via Len-then-Reserve TOCTOU.
func TestMaximumInflightReserveAtomic(t *testing.T) {
	const maxInflight = 32
	s := newServer()
	s.Options.Capabilities.MaximumInflight = maxInflight

	cl, r, w := newMQTT5QuotaTestClient(100, maxInflight)
	cl.ops.options.Capabilities.MaximumInflight = maxInflight
	cl.ops.options.Capabilities.maximumPacketID = 65535
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

	sub := packets.Subscription{Filter: "cap/t", Qos: 1}
	var wg sync.WaitGroup
	var accepted atomic.Int64
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				_, err := s.publishToClient(cl, sub, packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
					TopicName:   "cap/t",
					Payload:     []byte("x"),
					Created:     time.Now().Unix(),
				})
				if err == nil {
					accepted.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.LessOrEqual(t, cl.State.Inflight.Len(), maxInflight,
		"Inflight must not exceed MaximumInflight under concurrent publish")
	require.Greater(t, accepted.Load(), int64(0))
}

// TestResetSendQuotaIfEmptyNoRace: concurrent Reserve must not be undone by empty-window heal.
func TestResetSendQuotaIfEmptyNoRace(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.ResetSendQuota(8)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				cl.State.Inflight.ResetSendQuotaIfEmpty()
			}
		}()
		wg.Add(1)
		go func(id uint16) {
			defer wg.Done()
			if cl.State.Inflight.Reserve(id + 1) {
				cl.State.Inflight.DecreaseSendQuota()
			}
		}(uint16(i))
	}
	wg.Wait()

	length, sendQ, maxQ, _, _ := cl.State.Inflight.QuotaStats()
	if length > 0 {
		require.Less(t, sendQ, maxQ,
			"with reserved entries present, sendQuota must not have been reset to maximum")
	}
}

// TestClearExpiredSkipsSentinelsAndRestoresQuota covers expiry cleanup accounting.
func TestClearExpiredSkipsSentinelsAndRestoresQuota(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.ResetSendQuota(5)
	require.True(t, cl.State.Inflight.DecreaseSendQuota())
	require.True(t, cl.State.Inflight.DecreaseSendQuota())

	now := time.Now().Unix()
	cl.State.Inflight.Set(packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:        1,
		ProtocolVersion: 5,
		Created:         now - 100,
		Expiry:          now - 1, // expired
	})
	cl.State.Inflight.Set(packets.Packet{
		PacketID: 2,
		Expiry:   InflightExpiryDeferred,
		Created:  0,
	})
	cl.State.Inflight.Set(packets.Packet{
		PacketID: 3,
		Expiry:   InflightExpiryReserved,
		Created:  0,
	})
	atomic.StoreInt64(&cl.ops.info.Inflight, 3)

	deleted := cl.ClearExpiredInflights(now, 10)
	require.Equal(t, []uint16{1}, deleted)
	require.Equal(t, 2, cl.State.Inflight.Len(), "deferred+reserved must remain")
	_, sendQ, _, _, _ := cl.State.Inflight.QuotaStats()
	require.Equal(t, int32(4), sendQ, "sendQuota restored for expired sent message (5-2+1)")
}

// TestInboundPublishDoesNotDeleteOutboundInflight: packetid-clash fix.
func TestInboundPublishDoesNotDeleteOutboundInflight(t *testing.T) {
	s := newServer()
	cl, r, w := newMQTT5QuotaTestClient(8, 1024)
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

	outbound := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    7,
		TopicName:   "out",
		Payload:     []byte("keep-me"),
		Created:     time.Now().Unix(),
	}
	cl.State.Inflight.Set(outbound)
	cl.State.Inflight.ResetSendQuota(8)
	require.True(t, cl.State.Inflight.DecreaseSendQuota())
	atomic.StoreInt64(&s.Info.Inflight, 1)

	// Inbound PUBLISH reusing PacketID 7 must not wipe the outbound entry.
	err := s.processPublish(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    7,
		TopicName:   "in",
		Payload:     []byte("inbound"),
		Origin:      cl.ID,
	})
	require.NoError(t, err)

	got, ok := cl.State.Inflight.Get(7)
	require.True(t, ok, "outbound inflight must survive inbound PacketID reuse")
	require.Equal(t, "out", got.TopicName)
	require.Equal(t, []byte("keep-me"), got.Payload)
	_, sendQ, _, _, _ := cl.State.Inflight.QuotaStats()
	require.Equal(t, int32(7), sendQ, "sendQuota must not be leaked")
}

// TestSessionClonePreservesSendQuotaAccounting: session takeover must not reset to full window.
func TestSessionClonePreservesSendQuotaAccounting(t *testing.T) {
	src := NewInflights()
	src.ResetSendQuota(8)
	src.ResetReceiveQuota(10)
	for i := uint16(1); i <= 3; i++ {
		require.True(t, src.DecreaseSendQuota())
		src.Set(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    i,
			Created:     time.Now().Unix(),
		})
	}
	src.Set(packets.Packet{PacketID: 10, Expiry: InflightExpiryDeferred})
	src.Set(packets.Packet{PacketID: 11, Expiry: InflightExpiryReserved})

	cloned := src.Clone()
	require.Equal(t, 5, cloned.Len())
	_, sendQ, maxQ, _, _ := cloned.QuotaStats()
	require.Equal(t, int32(8), maxQ)
	require.Equal(t, int32(5), sendQ, "8 - 3 sent (deferred/reserved do not consume)")

	cloned.ReconcileSendQuotaCap(4)
	_, sendQ, maxQ, _, _ = cloned.QuotaStats()
	require.Equal(t, int32(4), maxQ)
	require.Equal(t, int32(1), sendQ, "4 - 3 sent")
}

// TestResumeDeferredPublishesDrainsWindow flushes up to sendQuota deferred msgs at once.
func TestResumeDeferredPublishesDrainsWindow(t *testing.T) {
	s := newServer()
	cl, r, w := newMQTT5QuotaTestClient(4, 1024)
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

	cl.State.Inflight.ResetSendQuota(4)
	for i := uint16(1); i <= 10; i++ {
		cl.State.Inflight.Set(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    i,
			TopicName:   "d",
			Payload:     []byte("x"),
			Created:     int64(i),
			Expiry:      InflightExpiryDeferred,
		})
		atomic.AddInt64(&s.Info.Inflight, 1)
	}

	require.NoError(t, s.resumeDeferredPublishes(cl))

	length, deferred, _, sendQ, _, _, _ := cl.State.Inflight.QuotaStatsDetailed()
	require.Equal(t, 10, length)
	require.Equal(t, 6, deferred, "4 deferred should have been promoted to sent")
	require.Equal(t, int32(0), sendQ)
}

// TestSessionTakeoverPreservesInflightGauge: Clone then ClearInflights used to decrement
// Info.Inflight while messages still lived on the new session — PUBACKs drove the gauge negative.
func TestSessionTakeoverPreservesInflightGauge(t *testing.T) {
	s := newServer()
	oldCl, _, _ := newTestClient()
	oldCl.Net.Conn = nil // avoid DisconnectClient WritePacket blocking in inherit
	oldCl.ID = "takeover-victim"
	oldCl.Properties.Clean = false
	oldCl.Properties.ProtocolVersion = 5
	oldCl.Properties.Props.SessionExpiryInterval = 120
	oldCl.Properties.Props.SessionExpiryIntervalFlag = true
	oldCl.Properties.Props.ReceiveMaximum = 8
	oldCl.ops.info = s.Info
	s.Clients.Add(oldCl)

	for i := uint16(1); i <= 2; i++ {
		require.True(t, oldCl.State.Inflight.ReplaceCounted(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    i,
			TopicName:   "t",
			Payload:     []byte("x"),
			Created:     time.Now().Unix(),
		}, &s.Info.Inflight))
	}
	require.Equal(t, int64(2), atomic.LoadInt64(&s.Info.Inflight))

	newCl, _, _ := newTestClient()
	newCl.ID = "takeover-victim"
	newCl.Properties.Clean = false
	newCl.Properties.ProtocolVersion = 5
	newCl.Properties.Props.ReceiveMaximum = 8
	newCl.ops.info = s.Info

	present := s.inheritClientSession(packets.Packet{
		Connect: packets.ConnectParams{ClientIdentifier: "takeover-victim", Clean: false},
	}, newCl)
	require.True(t, present)
	require.Equal(t, 2, newCl.State.Inflight.Len())
	require.Equal(t, 0, oldCl.State.Inflight.Len())
	require.Equal(t, int64(2), atomic.LoadInt64(&s.Info.Inflight),
		"gauge must stay at 2 after transfer; ClearInflights would have made it 0")

	// Completing both QoS flows should return the gauge to zero, not -2.
	for i := uint16(1); i <= 2; i++ {
		require.NoError(t, s.processPuback(newCl, packets.Packet{PacketID: i}))
	}
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.Inflight))
	require.Equal(t, 0, newCl.State.Inflight.Len())
}
