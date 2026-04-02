/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v2.0
 *  and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 *  and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.php.
 *
 *  SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
 */

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// MqttConfig 参数配置
type MqttConfig struct {
	Endpoint string `mapstructure:"endpoint"`
	QoS      byte   `mapstructure:"qos"`
}

// MqttClientSrv 是众多服务封装的一个类
type MqttClientSrv struct {
	Client   *autopaho.ConnectionManager
	Endpoint string
	QoS      byte
}

// NewMqttClient 是新构造一个服务对象
func NewMqttClient(c *MqttConfig, onConnUp func(cm *autopaho.ConnectionManager, connAck *paho.Connack)) (*MqttClientSrv, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		fmt.Println("NewMqttClient error:", err.Error(), "endpoint:", c.Endpoint)
		// zlog.Error("NewMqttClient", zlog.Fields{"error": err.Error(), "endpoint": c.Endpoint})
		return nil, err
	}

	client, err := GetMqttClient(context.Background(), u, onConnUp)
	if err != nil {
		fmt.Println("GetMqttClient error:", err.Error(), "endpoint:", c.Endpoint)
		// zlog.Error("GetMqttClient", zlog.Fields{"error": err.Error(), "endpoint": c.Endpoint})
		return nil, err
	}

	return &MqttClientSrv{
		Client:   client,
		Endpoint: c.Endpoint,
		QoS:      c.QoS,
	}, nil
}

// GetMqttClient if gen a client to connect broker
func GetMqttClient(ctx context.Context, serverURL *url.URL, onConnUp func(cm *autopaho.ConnectionManager, connAck *paho.Connack)) (*autopaho.ConnectionManager, error) {
	router := paho.NewStandardRouter()
	router.DefaultHandler(func(p *paho.Publish) { fmt.Printf("defaulthandler received message with topic: %s\n", p.Topic) })

	// a handler
	router.RegisterHandler("testtopic/#", func(p *paho.Publish) {
		fmt.Printf("testtopic/# received message with topic: %s, message : %s \n", p.Topic, string(p.Payload))
	})
	router.RegisterHandler("test/test/foo", func(p *paho.Publish) { fmt.Printf("test/test/foo received message with topic: %s\n", p.Topic) })
	router.RegisterHandler("test/nomatch", func(p *paho.Publish) { fmt.Printf("test/nomatch received message with topic: %s\n", p.Topic) })
	router.RegisterHandler("test/quit", func(p *paho.Publish) { os.Exit(1) }) // Context will be cancelled if we receive a matching message

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		KeepAlive:                     30,                                   // Keepalive message should be sent every 20 seconds
		CleanStartOnInitialConnection: true,                                 // Previous tests should not contaminate this one!
		ReconnectBackoff:              autopaho.DefaultExponentialBackoff(), // 添加指数退避策略
		SessionExpiryInterval:         60,                                   // If connection drops we want session to remain live whilst we reconnect   客户端连接的会话过期时间，单位秒
		OnConnectionUp:                onConnUp,
		// func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
		// 	zlog.Info("autopaho.ClientConfig", zlog.Fields{"msg": "mqtt connection up"})
		// 	// 订阅所有主题
		// 	for topic := range topicParamMap {
		// 		err := SubscribeTopic(cm, topic)
		// 		if err != nil {
		// 			zlog.Error("SubscribeTopic", zlog.Fields{"error": err.Error()})
		// 		}
		// 	}
		// 	// fmt.Println("publish: mqtt connection up")
		// },

		ConnectPacketBuilder: func(cp *paho.Connect, url *url.URL) (*paho.Connect, error) {
			// 设置 MQTT 协议版本: 3=MQTT 3.0, 4=MQTT 3.1.1, 5=MQTT 5.0
			if pkt := cp.Packet(); pkt != nil {
				pkt.ProtocolVersion = 4 // MQTT 3.1.1
				pkt.ProtocolName = "MQTT"
			}
			if cp.Properties == nil {
				cp.Properties = &paho.ConnectProperties{}
			}
			cp.Properties.ReceiveMaximum = paho.Uint16(100) // 设置最大并发数为100
			return cp, nil
		},
		OnConnectError: func(err error) {
			fmt.Printf("publish: error whilst attempting connection: %s\n", err)
			// zlog.Error("autopaho.ClientConfig", zlog.Fields{"msg": "OnConnectError", "err": err.Error()})
		},
		Errors:          logger{prefix: "publish"},
		Debug:           logger{prefix: "publish: debug"},
		PahoErrors:      logger{prefix: "publishP"},
		ConnectUsername: "push",
		ConnectPassword: []byte("password3"),
		// PahoDebug:       logger{prefix: "publishP: debug"},
		// eclipse/paho.golang/paho provides base mqtt functionality, the below config will be passed in for each connection
		ClientConfig: paho.ClientConfig{
			ClientID: "Push-Api",
			OnClientError: func(err error) {
				fmt.Printf("publish: client error: %s\n", err)
				// zlog.Error("OnClientError", zlog.Fields{"err": err.Error()})
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					// zlog.Error("OnServerDisconnect", zlog.Fields{"msg": "server requested disconnect", "reasion": d.Properties.ReasonString})
					fmt.Printf("publish: server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					// zlog.Error("OnServerDisconnect", zlog.Fields{"msg": "server requested disconnect", "reason code": d.ReasonCode})
					fmt.Printf("publish: server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
			PacketTimeout: 2 * time.Second, // 2 seconds
			/*
				OnPublishReceived: []func(paho.PublishReceived) (bool, error){
					func(pr paho.PublishReceived) (bool, error) {
						zlog.Debug("OnPublishReceived", zlog.Fields{"topic": pr.Packet.Topic, "payload": string(pr.Packet.Payload)})
						return true, nil // we assume that the router handles all messages (todo: amend router API)
					},
				},*/
		},
	}

	c, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		fmt.Printf("autopaho.NewConnection error: %s\n", err.Error())
		// zlog.Error("autopaho.NewConnection", zlog.Fields{"error": err.Error(), "host": serverURL.Host, "port": serverURL.Port()})
		return nil, err
	} else {
		return c, nil
	}
}

// MqttPayload pyload data
type MqttPayload struct {
	Domain    string `json:"domain"`    // 领域  IVI， ADAS
	Target    string `json:"target"`    // 目标
	Data      string `json:"data"`      // 消息
	MsgID     string `json:"msgID"`     // 消息ID
	ClientID  string `json:"clientID"`  // 客户端ID
	Timestamp int64  `json:"timestamp"` // 任务下发的时间
}

// SendMsg send msg to mqtt broker
// retain 如果为true，则表示该消息会持久化，后面的一条会替换前一条，一个topic最多只有一条
// expire 消息过期时间，到时间后，会删除
func (l *MqttClientSrv) SendMsg(ctx context.Context, topic string, payload *MqttPayload, retain bool, expire uint32) error {
	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("SendMsg json.Marshal, error : %s\n", err.Error())
		// zlog.Error("json.Marshal", zlog.Fields{"error": err.Error()})
		return err
	}

	// 在发布消息时使用队列方式
	err = l.Client.PublishViaQueue(ctx, &autopaho.QueuePublish{
		Publish: &paho.Publish{
			QoS:     l.QoS,
			Topic:   topic,
			Retain:  retain,
			Payload: data,
			Properties: &paho.PublishProperties{
				MessageExpiry: paho.Uint32(expire),
			},
		},
	})
	// _, err := l.Client.Publish(ctx, &paho.Publish{
	// 	QoS:     l.QoS,
	// 	Topic:   topic,
	// 	Retain:  retain,
	// 	Payload: payload,
	// 	Properties: &paho.PublishProperties{
	// 		MessageExpiry: paho.Uint32(expire),
	// 	},
	// })
	if err != nil {
		fmt.Printf("MqttClient.SendMsg, error : %s\n", err.Error())
		// zlog.Error("MqttClient.SendMsg", zlog.Fields{"error": err.Error()})
		return err
	}
	return nil
}

// SendMsg send msg to mqtt broker
// retain 如果为true，则表示该消息会持久化，后面的一条会替换前一条，一个topic最多只有一条
// expire 消息过期时间，到时间后，会删除,单位秒
func (l *MqttClientSrv) SendObject(ctx context.Context, topic string, payload interface{}, retain bool, expire uint32) error {
	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("SendObject json.Marshal, error : %s\n", err.Error())
		return err
	}

	_, err = l.Client.Publish(ctx, &paho.Publish{
		QoS:     l.QoS,
		Topic:   topic,
		Retain:  retain,
		Payload: data,
		Properties: &paho.PublishProperties{
			MessageExpiry: paho.Uint32(expire),
		},
	})
	if err != nil {
		fmt.Printf("MqttClient.SendMsg, error : %s\n", err.Error())
		return err
	}
	return nil
}

func (l *MqttClientSrv) Subscribe(ctx context.Context, topic string) error {
	_, err := l.Client.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				Topic:   topic,
				QoS:     1,
				NoLocal: true,
			},
		},
	})
	if err != nil {
		fmt.Printf("MqttClient.Subscribe, error : %s\n", err.Error())
		// zlog.Error("MqttClient.SendMsg", zlog.Fields{"error": err.Error()})
		return err
	}
	return nil
}
