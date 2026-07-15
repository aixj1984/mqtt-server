// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sort"
	"sync"

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
	i.Lock()
	defer i.Unlock()

	if _, ok := i.internal[id]; ok {
		return false
	}
	i.internal[id] = packets.Packet{
		PacketID: id,
		Expiry:   InflightExpiryReserved,
	}
	return true
}

// Replace stores the real packet over a previously Reserved id (or inserts if absent).
// Returns false only when overwriting a non-reserved existing message — that indicates a
// serious accounting bug and the caller must not treat the message as successfully stored.
func (i *Inflight) Replace(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()

	old, ok := i.internal[m.PacketID]
	i.internal[m.PacketID] = m
	if !ok {
		return true
	}
	return old.Expiry == InflightExpiryReserved
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

// Clone returns a new instance of Inflight with the same message data.
// This is used when transferring inflights from a taken-over session.
func (i *Inflight) Clone() *Inflight {
	c := NewInflights()
	i.RLock()
	defer i.RUnlock()
	for k, v := range i.internal {
		c.internal[k] = v
	}
	return c
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
	i.RLock()
	defer i.RUnlock()

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
	i.RLock()
	allDeferred := len(i.internal) > 0
	for _, v := range i.internal {
		if v.Expiry != InflightExpiryDeferred {
			allDeferred = false
			break
		}
	}
	i.RUnlock()
	if !allDeferred {
		return false
	}

	i.quotaMu.Lock()
	defer i.quotaMu.Unlock()
	if i.sendQuota > 0 || i.maximumSendQuota == 0 {
		return false
	}
	i.sendQuota = 1
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
