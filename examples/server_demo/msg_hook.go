package main

import (
	"bytes"
	"fmt"
	"regexp"

	mqtt "github.com/aixj1984/mqtt-server"
	"github.com/aixj1984/mqtt-server/packets"
)

// MsgHookOptions Options contains configuration settings for the hook.
type MsgHookOptions struct {
	Server *mqtt.Server
}

// MsgHook msg process hook
type MsgHook struct {
	mqtt.HookBase
	config *MsgHookOptions
}

// ID returns the ID of the hook.
func (h *MsgHook) ID() string {
	return "msg-process"
}

// Provides indicates which hook methods this hook provides.
func (h *MsgHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnect,
		// mqtt.OnACLCheck,
		mqtt.OnConnectAuthenticate,
		mqtt.OnPacketRead,
		mqtt.OnPacketSent,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
		mqtt.OnSubscribe,
		mqtt.OnUnsubscribed,
		mqtt.OnPublished,
		mqtt.OnPublish,
		mqtt.OnQosDropped,
		mqtt.OnPublishDropped,
		mqtt.OnQosComplete,
	}, []byte{b})
}

// Init performs any pre-start initializations for the hook, such as connecting to databases
// or opening files.
func (h *MsgHook) Init(config any) error {
	h.Log.Info("initialised")
	if _, ok := config.(*MsgHookOptions); !ok && config != nil {
		return mqtt.ErrInvalidConfigType
	}

	h.config = config.(*MsgHookOptions)
	if h.config.Server == nil {
		return mqtt.ErrInvalidConfigType
	}
	return nil
}

func (h *MsgHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	h.Log.Info("OnACLCheck",
		"client", cl.ID,
		"username", string(cl.Properties.Username),
		"topic", topic,
		"write", write)

	return false
}

// subscribeCallback handles messages for subscribed topics
// func (h *MsgHook) subscribeCallback(cl *mqtt.Client, sub packets.Subscription, pk packets.Packet) {
// 	h.Log.Info("hook subscribed message", "client", cl.ID, "topic", pk.TopicName)
// }

// OnConnect is called when a new client connects, and may return a packets.Code as an error to halt the connection.
func (h *MsgHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	h.Log.Info("--->OnConnect", "client", cl.ID, "payload", string(pk.Payload), "Origin", pk.Origin, "remote ip", cl.Net.Remote)

	tmpClient, exist := h.config.Server.Clients.Get(cl.ID)
	if exist {
		h.Log.Info(cl.ID + " already exists, disconnecting it")
		_ = h.config.Server.DisconnectClient(tmpClient, packets.ErrAdministrativeAction)
		h.config.Server.Clients.Delete(tmpClient.ID)
	}

	// // Example demonstrating how to subscribe to a topic within the hook.
	// h.config.Server.Subscribe("hook/direct/publish", 1, h.subscribeCallback)

	// // Example demonstrating how to publish a message within the hook
	// err := h.config.Server.Publish("hook/direct/publish", []byte("packet hook message"), false, 0)
	// if err != nil {
	// 	h.Log.Error("hook.publish", "error", err)
	// }

	return nil
}

// TopicParam pyload data
type TopicParam struct {
	Path     string `json:"aw"`       // 领域  IVI， ADAS
	ClientID string `json:"clientID"` // 应用
	Act      string `json:"act"`      // 消息
}

func topicParams(topic string) *TopicParam {
	pattern := `^/(.+?)/(.+?)/(.+?)$`

	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(topic)

	if match != nil {
		return &TopicParam{
			Path:     match[1],
			ClientID: match[2],
			Act:      match[3],
		}
		// fmt.Println("down:", match[1]) // 提取第一个括号里的内容
		// fmt.Println("client:", match[2])
		// fmt.Println("act:", match[3])
	} else {
		// fmt.Println("No match found")
		return nil
	}
}

// OnDisconnect is called when a client is disconnected for any reason.
func (h *MsgHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	if err != nil {
		h.Log.Info("OnDisconnect", "client", cl.ID, "expire", expire, "error", err)
	} else {
		h.Log.Info("OnDisconnect", "client", cl.ID, "expire", expire)
	}
}

// OnConnectAuthenticate is called when a user attempts to authenticate with the server.
// An implementation of this method MUST be used to allow or deny access to the
// server (see hooks/auth/allow_all or basic). It can be used in custom hooks to
// check connecting users against an existing user database.
func (h *MsgHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	h.Log.Info("--->OnConnectAuthenticate", "client", cl.ID, "payload", string(pk.Payload), "Origin", pk.Origin, "PacketID", pk.PacketID)
	return true
}

// OnSubscribe is called when a client subscribes to one or more filters. This method
// differs from OnSubscribed in that it allows you to modify the subscription values
// before the packet is processed. The return values of the hook methods are passed-through
// in the order the hooks were attached.
func (h *MsgHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	h.Log.Info("OnSubscribe", "client", cl.ID, "payload", string(pk.Payload), "topic", "Origin", pk.Origin, "PacketID", pk.PacketID, "filters", pk.Filters)
	return pk
}

// OnACLCheck is called when a user attempts to publish or subscribe to a topic filter.
// An implementation of this method MUST be used to allow or deny access to the
// (see hooks/auth/allow_all or basic). It can be used in custom hooks to
// check publishing and subscribing users against an existing permissions or roles database.
// func (h *MsgHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
// 	h.Log.Info("OnACLCheck", "client", cl.ID, "topic", topic, "write", write)
// 	return true
// }

// OnPacketRead is called when a packet is received from a client.
func (h *MsgHook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if len(pk.TopicName) > 0 {
		h.Log.Info("OnPacketRead", "client", cl.ID, "payload", string(pk.Payload), "topic", pk.TopicName, "Origin", pk.Origin, "PacketID", pk.PacketID)
	}

	return pk, nil
}

// OnPacketSent is called when a packet has been sent to a client. It takes a bytes parameter
// containing the bytes sent.
func (h *MsgHook) OnPacketSent(cl *mqtt.Client, pk packets.Packet, b []byte) {
	if len(pk.TopicName) == 0 {
		return
	}

	h.Log.Info("OnPacketSent", "client", cl.ID, "payload", string(pk.Payload), "topic", pk.TopicName, "Origin", pk.Origin, "PacketID", pk.PacketID)
}

// OnSubscribed is called when a client subscribes to one or more filters.
func (h *MsgHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	h.Log.Info(fmt.Sprintf("OnSubscribed qos=%v", reasonCodes), "client", cl.ID, "payload", string(pk.Payload), "Origin", pk.Origin, "PacketID", pk.PacketID, "filters", pk.Filters)
}

// OnUnsubscribed is called when a client unsubscribes from one or more filters.
func (h *MsgHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	h.Log.Info("OnUnsubscribed", "client", cl.ID, "payload", string(pk.Payload), "Origin", pk.Origin, "PacketID", pk.PacketID, "filters", pk.Filters)
}

// OnPublish is called when a client publishes a message. This method differs from OnPublished
// in that it allows you to modify you to modify the incoming packet before it is processed.
// The return values of the hook methods are passed-through in the order the hooks were attached.
func (h *MsgHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	h.Log.Info("OnPublish", "client", cl.ID, "payload", string(pk.Payload), "topic", pk.TopicName, "Origin", pk.Origin, "PacketID", pk.PacketID)

	return pk, nil
}

// OnPublished is called when a client has published a message to subscribers.
func (h *MsgHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	h.Log.Info("OnPublished", "client", cl.ID, "payload", string(pk.Payload), "topic", pk.TopicName, "Origin", pk.Origin, "PacketID", pk.PacketID)
}

// OnQosDropped is called the Qos flow for a message expires. In other words, when
// an inflight message expires or is abandoned. It is typically used to delete an
// inflight message from a store.
func (h *MsgHook) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	h.Log.Info("OnQosDropped", "client", cl.ID, "payload", string(pk.Payload), "topic", pk.TopicName, "Origin", pk.Origin, "PacketID", pk.PacketID, "Expiry", pk.Expiry)
}

// OnPublishDropped is called when a message to a client was dropped instead of delivered
// such as when a client is too slow to respond.
func (h *MsgHook) OnPublishDropped(cl *mqtt.Client, pk packets.Packet) {
	h.Log.Info("OnPublishDropped", "client", cl.ID, "payload", string(pk.Payload), "topic", pk.TopicName, "Origin", pk.Origin, "PacketID", pk.PacketID, "Expiry", pk.Expiry)
}

// OnQosComplete is called when the Qos flow for a message has been completed.
// In other words, when an inflight message is resolved.
// It is typically used to delete an inflight message from a store.
func (h *MsgHook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	h.Log.Info("OnQosComplete", "client", cl.ID, "payload", string(pk.Payload), "topic", pk.TopicName, "Origin", pk.Origin, "PacketID", pk.PacketID)
}
