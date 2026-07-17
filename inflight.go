// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aixj1984/mqtt-server/packets"
)

// Inflight Expiry sentinels reused for non-time-expiry flow-control states.
const (
	// InflightExpiryDeferred marks a QoS message parked until sendQuota is available (MQTT5).
	InflightExpiryDeferred int64 = -1
	// InflightExpiryReserved marks a packet ID claimed by NextPacketID before the real
	// publish payload is written — closes the TOCTOU window between ID selection and Set.
	InflightExpiryReserved int64 = -2
)

// reserveResult is the outcome of TryReserve.
type reserveResult int

const (
	reserveOK reserveResult = iota
	reserveIDTaken
	reserveFull
)

// Inflight is a map of InflightMessage keyed on packet id.
type Inflight struct {
	sync.RWMutex
	internal            map[uint16]packets.Packet // internal contains the inflight packets
	quotaMu             sync.Mutex                // protects quota fields
	receiveQuota        int32                     // remaining inbound qos quota for flow control
	sendQuota           int32                     // remaining outbound qos quota for flow control
	maximumReceiveQuota int32                     // maximum allowed receive quota
	maximumSendQuota    int32                     // maximum allowed send quota
}

// NewInflights returns a new instance of an Inflight packets map.
func NewInflights() *Inflight {
	return &Inflight{
		internal: map[uint16]packets.Packet{},
	}
}

// Set adds or updates an inflight packet by packet id.
func (i *Inflight) Set(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[m.PacketID]
	i.internal[m.PacketID] = m
	return !ok
}

// Reserve atomically claims a free packet id with a placeholder entry.
// Returns false if the id is already present (including a prior reservation).
func (i *Inflight) Reserve(id uint16) bool {
	return i.TryReserve(id, 0) == reserveOK
}

// TryReserve claims id if free. When max > 0, also requires len(internal) < max so
// MaximumInflight checks cannot race with concurrent publishers (Len-then-Reserve TOCTOU).
func (i *Inflight) TryReserve(id uint16, max int) reserveResult {
	i.Lock()
	defer i.Unlock()

	if max > 0 && len(i.internal) >= max {
		return reserveFull
	}
	if _, ok := i.internal[id]; ok {
		return reserveIDTaken
	}
	i.internal[id] = packets.Packet{
		PacketID: id,
		Expiry:   InflightExpiryReserved,
		Created:  time.Now().Unix(),
	}
	return reserveOK
}

// Replace stores the real packet over a previously Reserved id (or inserts if absent).
// Returns false only when overwriting a non-reserved existing message — that indicates a
// serious accounting bug and the caller must not treat the message as successfully stored.
func (i *Inflight) Replace(m packets.Packet) bool {
	return i.ReplaceCounted(m, nil)
}

// ReplaceCounted is Replace, and when gauge != nil increments it atomically under the same
// lock so ClearInflights cannot observe an uncounted real entry (Replace-then-+1 TOCTOU).
func (i *Inflight) ReplaceCounted(m packets.Packet, gauge *int64) bool {
	i.Lock()
	defer i.Unlock()

	old, ok := i.internal[m.PacketID]
	if ok && old.Expiry != InflightExpiryReserved {
		return false
	}
	i.internal[m.PacketID] = m
	if gauge != nil {
		atomic.AddInt64(gauge, 1)
	}
	return true
}

// Get returns an inflight packet by packet id.
func (i *Inflight) Get(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()

	if m, ok := i.internal[id]; ok {
		return m, true
	}

	return packets.Packet{}, false
}

// Len returns the size of the inflight messages map.
func (i *Inflight) Len() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.internal)
}

// Clone returns a new instance of Inflight with the same message data and recalculated quotas.
// Send quota is restored as maxSend - count(non-deferred, non-reserved QoS messages) so session
// takeover cannot reset to a full window while unacked outbound messages remain.
func (i *Inflight) Clone() *Inflight {
	c := NewInflights()
	i.RLock()
	sentHoldingQuota := 0
	for k, v := range i.internal {
		c.internal[k] = v
		if v.Expiry != InflightExpiryDeferred && v.Expiry != InflightExpiryReserved && v.FixedHeader.Qos > 0 {
			sentHoldingQuota++
		}
	}
	i.RUnlock()

	i.quotaMu.Lock()
	maxSend := i.maximumSendQuota
	maxRecv := i.maximumReceiveQuota
	recv := i.receiveQuota
	i.quotaMu.Unlock()

	c.quotaMu.Lock()
	c.maximumSendQuota = maxSend
	c.maximumReceiveQuota = maxRecv
	c.receiveQuota = recv
	if maxSend > 0 {
		left := maxSend - int32(sentHoldingQuota)
		if left < 0 {
			left = 0
		}
		c.sendQuota = left
	}
	c.quotaMu.Unlock()
	return c
}

// ReconcileSendQuotaCap updates maximumSendQuota and recomputes sendQuota from current
// non-deferred/non-reserved QoS entries. Used after session takeover when the new CONNECT
// advertises a different ReceiveMaximum.
func (i *Inflight) ReconcileSendQuotaCap(max int32) {
	i.RLock()
	sentHoldingQuota := 0
	for _, v := range i.internal {
		if v.Expiry != InflightExpiryDeferred && v.Expiry != InflightExpiryReserved && v.FixedHeader.Qos > 0 {
			sentHoldingQuota++
		}
	}
	i.RUnlock()

	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	if max <= 0 {
		return
	}
	i.maximumSendQuota = max
	left := max - int32(sentHoldingQuota)
	if left < 0 {
		left = 0
	}
	i.sendQuota = left
}

// GetAll returns all the inflight messages.
func (i *Inflight) GetAll(immediate bool) []packets.Packet {
	i.RLock()
	defer i.RUnlock()

	m := []packets.Packet{}
	for _, v := range i.internal {
		if !immediate || v.Expiry == InflightExpiryDeferred {
			m = append(m, v)
		}
	}

	sort.Slice(m, func(i, j int) bool {
		return uint16(m[i].Created) < uint16(m[j].Created)
	})

	return m
}

// NextImmediate returns the next inflight packet which is indicated to be sent immediately.
// This typically occurs when the quota has been exhausted, and we need to wait until new quota
// is free to continue sending.
func (i *Inflight) NextImmediate() (packets.Packet, bool) {
	m := i.GetAll(true)
	if len(m) > 0 {
		return m[0], true
	}

	return packets.Packet{}, false
}

// TakeNextImmediate selects the next deferred (Expiry=-1) packet and clears the deferred
// marker so it will not be selected again. The packet remains in the inflight map until the
// client acknowledges it. Returns false if there is no deferred packet.
func (i *Inflight) TakeNextImmediate() (packets.Packet, bool) {
	i.Lock()
	defer i.Unlock()

	var selected packets.Packet
	var selectedID uint16
	found := false
	for id, v := range i.internal {
		if v.Expiry != InflightExpiryDeferred {
			continue
		}
		if !found || uint16(v.Created) < uint16(selected.Created) {
			selected = v
			selectedID = id
			found = true
		}
	}
	if !found {
		return packets.Packet{}, false
	}

	// Clear deferred marker; keep packet until PUBACK/PUBCOMP restores send quota.
	selected.Expiry = 0
	i.internal[selectedID] = selected
	return selected, true
}

// RecoverStarvedSendQuota restores one send quota unit when every inflight packet is still
// deferred (Expiry=-1) and sendQuota has reached 0. This is unreachable with correct
// accounting, but recovers connections that entered the historical "all deferred, zero quota"
// deadlock without requiring a reconnect.
func (i *Inflight) RecoverStarvedSendQuota() bool {
	i.Lock()
	defer i.Unlock()

	if len(i.internal) == 0 {
		return false
	}
	for _, v := range i.internal {
		if v.Expiry != InflightExpiryDeferred {
			return false
		}
	}

	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	if i.sendQuota > 0 || i.maximumSendQuota == 0 {
		return false
	}
	i.sendQuota = 1
	return true
}

// ResetSendQuotaIfEmpty sets sendQuota to maximumSendQuota only when the inflight map is
// still empty. Holding the map lock closes the race where a concurrent publish Reserves an
// id and Decreases sendQuota, then this heal path would overwrite it back to max.
func (i *Inflight) ResetSendQuotaIfEmpty() bool {
	i.Lock()
	defer i.Unlock()
	if len(i.internal) != 0 {
		return false
	}

	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	if i.maximumSendQuota <= 0 {
		return false
	}
	if i.sendQuota == i.maximumSendQuota {
		return false
	}
	i.sendQuota = i.maximumSendQuota
	return true
}

// Delete removes an in-flight message from the map. Returns true if the message existed.
func (i *Inflight) Delete(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[id]
	delete(i.internal, id)

	return ok
}

// DecreaseReceiveQuota reduces the receive quota by 1.
// Returns true if quota was successfully decreased, false if quota was already 0.
func (i *Inflight) DecreaseReceiveQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.receiveQuota <= 0 {
		return false
	}
	i.receiveQuota--
	return true
}

// IncreaseReceiveQuota increases the receive quota by 1.
// Returns true if quota was successfully increased, false if already at maximum.
func (i *Inflight) IncreaseReceiveQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.receiveQuota >= i.maximumReceiveQuota {
		return false
	}
	i.receiveQuota++
	return true
}

// ResetReceiveQuota resets the receive quota to the maximum allowed value.
func (i *Inflight) ResetReceiveQuota(n int32) {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	i.maximumReceiveQuota = n
	i.receiveQuota = n
}

// DecreaseSendQuota reduces the send quota by 1.
// Returns true if quota was successfully decreased, false if quota was already 0.
func (i *Inflight) DecreaseSendQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.sendQuota <= 0 {
		return false
	}
	i.sendQuota--
	return true
}

// IncreaseSendQuota increases the send quota by 1.
// Returns true if quota was successfully increased, false if already at maximum.
func (i *Inflight) IncreaseSendQuota() bool {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	if i.sendQuota >= i.maximumSendQuota {
		return false
	}
	i.sendQuota++
	return true
}

// ResetSendQuota resets the send quota to the maximum allowed value.
func (i *Inflight) ResetSendQuota(n int32) {
	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()

	i.maximumSendQuota = n
	i.sendQuota = n
}

// QuotaStats returns a point-in-time snapshot of inflight length and flow-control quotas.
// Intended for diagnostics / race-repro demos (not on the hot path).
func (i *Inflight) QuotaStats() (length int, sendQuota, maxSendQuota, receiveQuota, maxReceiveQuota int32) {
	i.RLock()
	length = len(i.internal)
	i.RUnlock()

	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	return length, i.sendQuota, i.maximumSendQuota, i.receiveQuota, i.maximumReceiveQuota
}

// QuotaStatsDetailed returns QuotaStats plus deferred/reserved entry counts.
func (i *Inflight) QuotaStatsDetailed() (length, deferred, reserved int, sendQuota, maxSendQuota, receiveQuota, maxReceiveQuota int32) {
	i.RLock()
	length = len(i.internal)
	for _, v := range i.internal {
		switch v.Expiry {
		case InflightExpiryDeferred:
			deferred++
		case InflightExpiryReserved:
			reserved++
		}
	}
	i.RUnlock()

	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	return length, deferred, reserved, i.sendQuota, i.maximumSendQuota, i.receiveQuota, i.maximumReceiveQuota
}
