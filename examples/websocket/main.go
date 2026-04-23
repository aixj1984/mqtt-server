// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/aixj1984/mqtt-server"
	"github.com/aixj1984/mqtt-server/hooks/auth"
	"github.com/aixj1984/mqtt-server/listeners"
)

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	// server := mqtt.New(nil)
	server := mqtt.New(&mqtt.Options{
		Capabilities: &mqtt.Capabilities{
			ReceiveMaximum:               1024,         // maximum number of concurrent qos messages per client
			MaximumSessionExpiryInterval: 60,           // maximum number of seconds to keep disconnected sessions
			MaximumClients:               10000,        // maximum number of connected clients
			MaximumMessageExpiryInterval: 60 * 60 * 24, // maximum message expiry if message expiry is 0 or over
			MaximumClientWritesPending:   1024 * 8,     // maximum number of pending message writes for a client
			MaximumPacketSize:            1024 * 1024,  // no maximum packet size
			MaximumInflight:              1024 * 8,     // maximum number of qos > 0 messages can be stored
			TopicAliasMaximum:            111,          // maximum topic alias value
			SharedSubAvailable:           1,            // shared subscriptions are available
			MinimumProtocolVersion:       3,            // minimum supported mqtt version (3.0.0)
			MaximumQos:                   2,            // maximum qos value available to clients
			RetainAvailable:              1,            // retain messages is available
			WildcardSubAvailable:         1,            // wildcard subscriptions are available
			SubIDAvailable:               1,            // subscription identifiers are available
			Compatibilities: mqtt.Compatibilities{
				ObscureNotAuthorized: false,
			},
		},

		ClientNetWriteBufferSize: 40960,
		ClientNetReadBufferSize:  40960,
		SysTopicResendInterval:   60,
		InlineClient:             true,
		// Logger:                   slog.New(logHandler),
	})
	_ = server.AddHook(new(auth.AllowHook), nil)

	ws := listeners.NewWebsocket(listeners.Config{
		ID:      "ws1",
		Address: ":1882",
	})
	err := server.AddListener(ws)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	<-done
	server.Log.Warn("caught signal, stopping...")
	_ = server.Close()
	server.Log.Info("main.go finished")
}
