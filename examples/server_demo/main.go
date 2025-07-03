// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/aixj1984/mqtt-server"
	"github.com/aixj1984/mqtt-server/hooks/auth"
	"github.com/aixj1984/mqtt-server/hooks/storage/pebble"
	"github.com/aixj1984/mqtt-server/listeners"
	"github.com/aixj1984/mqtt-server/packets"
)

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	server := mqtt.New(&mqtt.Options{
		Capabilities: &mqtt.Capabilities{
			ReceiveMaximum:               1024,         // maximum number of concurrent qos messages per client
			MaximumSessionExpiryInterval: 60,           // maximum number of seconds to keep disconnected sessions
			MaximumClients:               10000,        // maximum number of connected clients
			MaximumMessageExpiryInterval: 60 * 60 * 24, // maximum message expiry if message expiry is 0 or over
			MaximumClientWritesPending:   1024 * 8,     // maximum number of pending message writes for a client
			MaximumPacketSize:            0,            // no maximum packet size
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
		SysTopicResendInterval:   10,
		InlineClient:             false,
	})

	_ = server.AddHook(new(auth.AllowHook), nil)

	level := new(slog.LevelVar)
	server.Log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	level.Set(slog.LevelDebug)

	// Add custom hook (ExampleHook) to the server
	err := server.AddHook(new(MsgHook), &MsgHookOptions{
		Server: server,
	})
	if err != nil {
		log.Fatal(err)
	}
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: ":1883",
	})
	err = server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	tmpClient, exist := server.Clients.Get("test_client_id")
	if exist {
		log.Println("test_client_id already exists, disconnecting it")
		_ = server.DisconnectClient(tmpClient, packets.ErrAdministrativeAction)
		server.Clients.Delete(tmpClient.ID)
	}

	err = server.AddHook(new(pebble.Hook), &pebble.Options{
		Path: "./data",
		Mode: pebble.Sync,
	})
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
