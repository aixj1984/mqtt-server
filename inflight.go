// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/aixj1984/mqtt-server/packets"
)

// Inflight is a map of InflightMessage keyed on packet id.
type Inflight struct {
	sync.RWMutex
	internal            map[uint16]packets.Packet // internal contains the inflight packets
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
		if !immediate || (immediate && v.Expiry < 0) {
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
	for {
		current := atomic.LoadInt32(&i.receiveQuota)
		if current <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&i.receiveQuota, current, current-1) {
			return true
		}
		// CAS failed, another goroutine modified the value, retry
	}
}

// IncreaseReceiveQuota increases the receive quota by 1.
// Returns true if quota was successfully increased, false if already at maximum.
func (i *Inflight) IncreaseReceiveQuota() bool {
	for {
		current := atomic.LoadInt32(&i.receiveQuota)
		max := atomic.LoadInt32(&i.maximumReceiveQuota)
		if current >= max {
			return false
		}
		if atomic.CompareAndSwapInt32(&i.receiveQuota, current, current+1) {
			return true
		}
		// CAS failed, another goroutine modified the value, retry
	}
}

// ResetReceiveQuota resets the receive quota to the maximum allowed value.
func (i *Inflight) ResetReceiveQuota(n int32) {
	atomic.StoreInt32(&i.maximumReceiveQuota, n)
	atomic.StoreInt32(&i.receiveQuota, n)
}

// DecreaseSendQuota reduces the send quota by 1.
// Returns true if quota was successfully decreased, false if quota was already 0.
func (i *Inflight) DecreaseSendQuota() bool {
	for {
		current := atomic.LoadInt32(&i.sendQuota)
		if current <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&i.sendQuota, current, current-1) {
			return true
		}
		// CAS failed, another goroutine modified the value, retry
	}
}

// IncreaseSendQuota increases the send quota by 1.
// Returns true if quota was successfully increased, false if already at maximum.
func (i *Inflight) IncreaseSendQuota() bool {
	for {
		current := atomic.LoadInt32(&i.sendQuota)
		max := atomic.LoadInt32(&i.maximumSendQuota)
		if current >= max {
			return false
		}
		if atomic.CompareAndSwapInt32(&i.sendQuota, current, current+1) {
			return true
		}
		// CAS failed, another goroutine modified the value, retry
	}
}

// ResetSendQuota resets the send quota to the maximum allowed value.
func (i *Inflight) ResetSendQuota(n int32) {
	atomic.StoreInt32(&i.maximumSendQuota, n)
	atomic.StoreInt32(&i.sendQuota, n)
}
