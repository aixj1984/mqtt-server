// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt contributors

package mqtt

import (
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aixj1984/mqtt-server/packets"
	"github.com/aixj1984/mqtt-server/system"
	"github.com/stretchr/testify/require"
)

// buggyResumeAfterPuback reproduces the historical broker bug:
// after PUBACK increases sendQuota, a deferred (Expiry=-1) packet is written and then
// immediately deleted from Inflight. The client's later PUBACK finds nothing, so
// sendQuota is never restored — once every actually-sent packet is acked, the
// connection is permanently stuck with sendQuota=0 and only parked messages left.
func buggyResumeAfterPuback(s *Server, cl *Client, packetID uint16) error {
	if ok := cl.State.Inflight.Delete(packetID); ok {
		cl.State.Inflight.IncreaseSendQuota()
		atomic.AddInt64(&s.Info.Inflight, -1)
	}

	if cl.State.Inflight.Len() > 0 && atomic.LoadInt32(&cl.State.Inflight.sendQuota) > 0 {
		next, ok := cl.State.Inflight.NextImmediate()
		if ok {
			_ = cl.WritePacket(next)
			if ok := cl.State.Inflight.Delete(next.PacketID); ok {
				atomic.AddInt64(&s.Info.Inflight, -1)
			}
			cl.State.Inflight.DecreaseSendQuota()
		}
	}
	return nil
}

func newMQTT5QuotaTestClient(receiveMaximum int32, maxInflight uint16) (cl *Client, r net.Conn, w net.Conn) {
	r, w = net.Pipe()
	cl = newClient(w, &ops{
		info:  new(system.Info),
		hooks: new(Hooks),
		log:   logger,
		options: &Options{
			Capabilities: &Capabilities{
				ReceiveMaximum:             uint16(receiveMaximum),
				MaximumInflight:            maxInflight,
				TopicAliasMaximum:          10000,
				MaximumClientWritesPending: 256,
				maximumPacketID:            1000,
			},
		},
	})
	cl.ID = "control-api-quota-repro"
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.ReceiveMaximum = uint16(receiveMaximum)
	cl.State.Inflight.ResetSendQuota(receiveMaximum)
	cl.State.Inflight.ResetReceiveQuota(receiveMaximum)
	go cl.WriteLoop()
	return cl, r, w
}

func publishBulkQoS1(t *testing.T, s *Server, cl *Client, n int) (sent []uint16, parked []uint16) {
	t.Helper()
	sub := packets.Subscription{Filter: "repro/quota", Qos: 1}
	for i := 0; i < n; i++ {
		pk := packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			TopicName:   "repro/quota",
			Payload:     []byte(fmt.Sprintf("msg-%d", i)),
			Created:     time.Now().Unix(),
		}
		out, err := s.publishToClient(cl, sub, pk)
		require.NoError(t, err, "publish %d", i)
		require.NotZero(t, out.PacketID)
		if out.Expiry < 0 {
			parked = append(parked, out.PacketID)
		} else {
			sent = append(sent, out.PacketID)
		}
	}
	return sent, parked
}

func firstAckablePacketID(cl *Client) (uint16, bool) {
	for _, pk := range cl.State.Inflight.GetAll(false) {
		if pk.Expiry >= 0 && pk.FixedHeader.Qos > 0 {
			return pk.PacketID, true
		}
	}
	return 0, false
}

func countDeferred(cl *Client) (deferred, sentWaitingAck int) {
	for _, pk := range cl.State.Inflight.GetAll(false) {
		if pk.Expiry < 0 {
			deferred++
		} else {
			sentWaitingAck++
		}
	}
	return deferred, sentWaitingAck
}

// TestMQTT5SendQuotaDeadlock_ReproWithBuggyResume shows the historical failure mode
// that matched production: after acking every actually-sent packet under the old
// resume logic, sendQuota stays 0 and parked messages never drain.
func TestMQTT5SendQuotaDeadlock_ReproWithBuggyResume(t *testing.T) {
	const (
		receiveMaximum = 2
		totalMsgs      = 10
	)

	s := newServer()
	s.Options.Capabilities.MaximumInflight = 1024
	cl, r, w := newMQTT5QuotaTestClient(receiveMaximum, 1024)
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

	sent, parked := publishBulkQoS1(t, s, cl, totalMsgs)
	require.Len(t, sent, receiveMaximum, "first ReceiveMaximum messages should be sent")
	require.Len(t, parked, totalMsgs-receiveMaximum, "overflow must be parked with Expiry=-1")
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, totalMsgs, cl.State.Inflight.Len())

	// Ack only packets that remain in Inflight as "actually sent".
	// Under the buggy resume path each ACK briefly frees quota, resumes one parked
	// message, then deletes it — burning the quota permanently.
	ackRounds := 0
	for {
		id, ok := firstAckablePacketID(cl)
		if !ok {
			break
		}
		require.NoError(t, buggyResumeAfterPuback(s, cl, id))
		ackRounds++
		require.Less(t, ackRounds, totalMsgs+2, "buggy path should stall before draining all messages")
	}

	deferred, waiting := countDeferred(cl)
	t.Logf("after buggy ACK loop: ackRounds=%d inflight=%d deferred=%d waitingAck=%d sendQuota=%d",
		ackRounds, cl.State.Inflight.Len(), deferred, waiting, atomic.LoadInt32(&cl.State.Inflight.sendQuota))

	require.Equal(t, receiveMaximum, ackRounds, "only the originally-sent window can be acked")
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, 0, waiting, "no in-flight packets left that can produce a PUBACK")
	require.Greater(t, deferred, 0, "parked messages remain forever")
	require.Equal(t, totalMsgs-receiveMaximum*2, deferred,
		"after resuming ReceiveMaximum parked msgs (and deleting them), the remainder stays deferred")

	// Further inbound packets cannot make progress either: sendQuota is still 0 and
	// RecoverStarvedSendQuota is intentionally not invoked in this buggy path.
	require.NoError(t, buggyResumeAfterPuback(s, cl, 0))
	require.Equal(t, deferred, cl.State.Inflight.Len())
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
}

// TestMQTT5SendQuotaDeadlock_FixedPathDrainsCompletely runs the same flood scenario
// through the real processPacket/PUBACK path and asserts the connection fully recovers:
// every parked message is delivered and sendQuota returns to ReceiveMaximum.
func TestMQTT5SendQuotaDeadlock_FixedPathDrainsCompletely(t *testing.T) {
	const (
		receiveMaximum = 2
		totalMsgs      = 10
	)

	s := newServer()
	s.Options.Capabilities.MaximumInflight = 1024
	cl, r, w := newMQTT5QuotaTestClient(receiveMaximum, 1024)
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

	sent, parked := publishBulkQoS1(t, s, cl, totalMsgs)
	require.Len(t, sent, receiveMaximum)
	require.Len(t, parked, totalMsgs-receiveMaximum)
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.Inflight.sendQuota))

	deadline := time.Now().Add(5 * time.Second)
	acked := 0
	for cl.State.Inflight.Len() > 0 {
		deferred, waiting := countDeferred(cl)
		require.True(t, time.Now().Before(deadline),
			"fixed path deadlocked: inflight=%d sendQuota=%d deferred=%d waiting=%d",
			cl.State.Inflight.Len(), atomic.LoadInt32(&cl.State.Inflight.sendQuota), deferred, waiting)

		id, ok := firstAckablePacketID(cl)
		require.True(t, ok, "expected an ackable in-flight packet; deferred=%d waiting=%d sendQuota=%d",
			deferred, waiting, atomic.LoadInt32(&cl.State.Inflight.sendQuota))

		err := s.processPacket(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    id,
		})
		require.NoError(t, err)
		acked++
	}

	require.Equal(t, totalMsgs, acked, "every published message must be acked exactly once")
	require.Equal(t, 0, cl.State.Inflight.Len())
	require.Equal(t, int32(receiveMaximum), atomic.LoadInt32(&cl.State.Inflight.sendQuota),
		"sendQuota must be fully restored after the window drains")
}

// TestMQTT5SendQuotaDeadlock_SafetyValveUnblocksAlreadyStuckConnection covers the
// production "already deadlocked" state: sendQuota=0 and Inflight contains only
// Expiry=-1 entries. A PINGREQ through the fixed path must resume traffic.
func TestMQTT5SendQuotaDeadlock_SafetyValveUnblocksAlreadyStuckConnection(t *testing.T) {
	const parkedCount = 8

	s := newServer()
	cl, r, w := newMQTT5QuotaTestClient(2, 1024)
	s.Clients.Add(cl)
	cl.State.Inflight.sendQuota = 0

	for i := 0; i < parkedCount; i++ {
		pk := packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    uint16(i + 1),
			TopicName:   "repro/quota",
			Payload:     []byte(fmt.Sprintf("stuck-%d", i)),
			Created:     int64(i + 1),
			Expiry:      -1,
		}
		cl.State.Inflight.Set(pk)
		atomic.AddInt64(&s.Info.Inflight, 1)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, r)
	}()
	defer func() {
		_ = w.Close()
		<-done
	}()

	require.Equal(t, parkedCount, cl.State.Inflight.Len())
	deferred, waiting := countDeferred(cl)
	require.Equal(t, parkedCount, deferred)
	require.Equal(t, 0, waiting)

	// One keepalive is enough for RecoverStarvedSendQuota to hand out a slot and
	// TakeNextImmediate to resume the oldest parked publish.
	require.NoError(t, s.processPacket(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pingreq},
	}))

	deferred, waiting = countDeferred(cl)
	require.Equal(t, parkedCount-1, deferred)
	require.Equal(t, 1, waiting)
	require.Equal(t, int32(0), atomic.LoadInt32(&cl.State.Inflight.sendQuota))

	// From here the normal PUBACK window can drain the rest.
	deadline := time.Now().Add(5 * time.Second)
	for cl.State.Inflight.Len() > 0 {
		require.True(t, time.Now().Before(deadline), "safety-valve recovery stalled")
		id, ok := firstAckablePacketID(cl)
		require.True(t, ok)
		require.NoError(t, s.processPacket(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    id,
		}))
	}
	require.Equal(t, 0, cl.State.Inflight.Len())
	require.Equal(t, int32(2), atomic.LoadInt32(&cl.State.Inflight.sendQuota),
		"empty inflight must reset sendQuota back to ReceiveMaximum")
}
