// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"bytes"
	"fmt"

	mqtt "github.com/aixj1984/mqtt-server"
	"github.com/aixj1984/mqtt-server/hooks/auth"
	"github.com/aixj1984/mqtt-server/packets"
)

// AuthHookOptions contains the configuration/rules data for the auth ledger.
type AuthHookOptions struct {
	Data   []byte
	Ledger *auth.Ledger
	Server *mqtt.Server
}

// AuthHook is an authentication hook which implements an auth ledger.
type AuthHook struct {
	mqtt.HookBase
	config *AuthHookOptions
	ledger *auth.Ledger
}

// ID returns the ID of the hook.
func (h *AuthHook) ID() string {
	return "auth-ledger"
}

// Provides indicates which hook methods this hook provides.
func (h *AuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnect,
		mqtt.OnDisconnect,
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
	}, []byte{b})
}

// Init configures the hook with the auth ledger to be used for checking.
func (h *AuthHook) Init(config any) error {
	if _, ok := config.(*AuthHookOptions); !ok && config != nil {
		return mqtt.ErrInvalidConfigType
	}

	if config == nil {
		config = new(AuthHookOptions)
	}

	h.config = config.(*AuthHookOptions)

	var err error
	if h.config.Ledger != nil {
		h.ledger = h.config.Ledger
	} else if len(h.config.Data) > 0 {
		h.ledger = new(auth.Ledger)
		err = h.ledger.Unmarshal(h.config.Data)
	}
	if err != nil {
		return err
	}

	if h.ledger == nil {
		h.ledger = &auth.Ledger{
			Auth: auth.AuthRules{},
			ACL:  auth.ACLRules{},
		}
	}

	h.Log.Info("loaded auth rules",
		"authentication", len(h.ledger.Auth),
		"acl", len(h.ledger.ACL))

	return nil
}

// OnConnect 连接建立时调用
func (h *AuthHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	h.Log.Info("--->OnConnect", "client", cl.ID, "payload", string(pk.Payload), "Origin", pk.Origin, "remote ip", cl.Net.Remote)

	// // Example demonstrating how to subscribe to a topic within the hook.
	// h.config.Server.Subscribe("hook/direct/publish", 1, h.subscribeCallback)

	// // Example demonstrating how to publish a message within the hook
	// err := h.config.Server.Publish("hook/direct/publish", []byte("packet hook message"), false, 0)
	// if err != nil {
	// 	h.Log.Error("hook.publish", "error", err)
	// }

	return nil
}

// OnConnectAuthenticate returns true if the connecting client has rules which provide access
// in the auth ledger.
func (h *AuthHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	h.Log.Info("OnConnectAuthenticate",
		"client", cl.ID,
		"username", string(pk.Connect.Username),
		"remote", cl.Net.Remote,
		"password", string(pk.Connect.Password),
		"payload", string(pk.Payload),
		"real ip", cl.Net.Conn.RemoteAddr().String(),
	)

	if n, ok := h.ledger.AuthOk(cl, pk); ok {
		fmt.Println("Auth rule matched:", h.ledger.Auth[n], "allow:", ok)
		return true
	}

	h.Log.Info("OnConnectAuthenticate failed authentication check",
		"username", string(pk.Connect.Username),
		"client", cl.ID,
		"remote", cl.Net.Remote)
	return false
}

// OnACLCheck returns true if the connecting client has matching read or write access to subscribe
// or publish to a given topic.
func (h *AuthHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	h.Log.Info("OnACLCheck",
		"client", cl.ID,
		"username", string(cl.Properties.Username),
		"topic", topic,
		"write", write)

	if _, ok := h.ledger.ACLOk(cl, topic, write); ok {
		return true
	}

	h.Log.Debug("OnACLCheck client failed allowed ACL check",
		"client", cl.ID,
		"username", string(cl.Properties.Username),
		"topic", topic,
		"write", write)

	return false
}

// OnDisconnect .
func (h *AuthHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	if err != nil {
		h.Log.Info("OnDisconnect", "client", cl.ID, "expire", expire, "error", err)
	} else {
		h.Log.Info("OnDisconnect", "client", cl.ID, "expire", expire)
	}
}
