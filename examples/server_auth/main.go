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

	//_ = server.AddHook(new(auth.AllowHook), nil)
	err := server.AddHook(new(AuthHook), &AuthHookOptions{
		Server: server,
		Ledger: &auth.Ledger{
			Auth: auth.AuthRules{ // Auth 默认情况下禁止所有连接
				{Username: "terminal", Password: "password1", Allow: true},
				{Username: "sdk", Password: "password2", Allow: true},
				{Remote: "127.0.0.1:*", Allow: false},
				//{Remote: "localhost:*", Allow: true},
			},
			ACL: auth.ACLRules{ // ACL 默认情况下允许所有连接
				//{Remote: "127.0.0.1:*"}, // 本地用户允许所有连接
				{
					// app  用户可以读取和写入自己的主题
					Username: "terminal", Filters: auth.Filters{
						"/terminal/+/sync":    auth.WriteOnly, // 同步设备配置
						"/terminal/+/setting": auth.ReadOnly,  // 获取设备配置
						"/terminal/+/up":      auth.WriteOnly, // 终端上报指令
						"/terminal/+/down":    auth.ReadOnly,  // 云端下发指令
						"/terminal/+/notice":  auth.ReadOnly,  // 云端下发全局通知
						// 测试向SDK发消息
						"/sdk/+/setting": auth.WriteOnly, // 获取SDK配置
						"/sdk/+/down":    auth.WriteOnly, // 云端下发指令
						"/sdk/+/notice":  auth.WriteOnly, // 云端下发全局通知

						"$SYS/#": auth.Deny, // 系统主题
					},
				},
				{
					// sdk 用户可以读取和写入自己的主题
					Username: "sdk", Filters: auth.Filters{
						"/sdk/+/setting": auth.ReadOnly,  // 获取SDK配置
						"/sdk/+/sync":    auth.WriteOnly, // 上报SDK配置
						"/sdk/+/up":      auth.WriteOnly, // SDK上报云端
						"/sdk/+/down":    auth.ReadOnly,  // 云端下发指令
						"/sdk/+/notice":  auth.ReadOnly,  // 云端下发全局通知
						// 测试向终端发消息
						"/terminal/+/setting": auth.WriteOnly, // 获取设备配置
						"/terminal/+/down":    auth.WriteOnly, // 云端下发指令
						"/terminal/+/notice":  auth.WriteOnly, // 云端下发全局通知

						"$SYS/#": auth.Deny, // 系统主题
					},
				},
				{
					// 其他的客户端没有发布的权限
					Filters: auth.Filters{
						"down/#": auth.ReadOnly,
						"up/#":   auth.Deny,
						"#":      auth.Deny,
					},
				},
			},
		},
	},
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	level := new(slog.LevelVar)
	server.Log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	level.Set(slog.LevelDebug)

	// Add custom hook (ExampleHook) to the server
	err = server.AddHook(new(MsgHook), &MsgHookOptions{
		Server: server,
	})
	if err != nil {
		log.Fatal(err)
	}
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: ":8883",
	})
	err = server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
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
