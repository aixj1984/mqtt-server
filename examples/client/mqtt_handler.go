package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// topicParamMap 存储主题模式和对应参数名的映射
var topicParamMap = map[string][]string{
	"/user/+/+/online":  {"userId", "deviceId"},
	"/user/+/+/offline": {"userId", "deviceId"},
	"/car/+/online":     {"carId"},
	"/car/+/offline":    {"carId"},
}

// parseTopic 解析主题，提取动态参数
func parseTopic(topic string, pattern string) map[string]string {
	paramNames, exists := topicParamMap[pattern]
	if !exists {
		return nil
	}

	// 将 + 替换为正则表达式的捕获组
	rePattern := regexp.MustCompile(`\+`)
	regexPattern := "^" + rePattern.ReplaceAllString(pattern, `([^/]+)`) + "$"
	re := regexp.MustCompile(regexPattern)
	values := re.FindStringSubmatch(topic)

	params := make(map[string]string)
	if len(values) > 1 {
		for i, name := range paramNames {
			if i+1 < len(values) {
				params[name] = values[i+1]
			}
		}
	}
	return params
}

// GetSetupFunc 获取 MQTT 连接设置函数
func GetSetupFunc() func(*autopaho.ConnectionManager, *paho.Connack) {
	return func(cm *autopaho.ConnectionManager, connack *paho.Connack) {
		// 添加消息处理器
		router := RegisterMqttHandlers(cm)
		cm.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
			go router.Route(pr.Packet.Packet())
			return true, nil
		})

		// 重新建立订阅
		cm.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{
				{Topic: "/user/+/+/online", QoS: 1, NoLocal: true},
				{Topic: "/user/+/+/offline", QoS: 1, NoLocal: true},
				{Topic: "/car/+/online", QoS: 1, NoLocal: true},
				{Topic: "/car/+/offline", QoS: 1, NoLocal: true},
			},
		})

		// 其他初始化操作
	}
}

// 定义外部函数

// RegisterMqttHandlers 注册 MQTT 消息处理的路由
func RegisterMqttHandlers(mqttClient *autopaho.ConnectionManager) *paho.StandardRouter {
	handler := MqttHandler{
		MqttClient: mqttClient,
		// ServerCtx:  serverCtx,
	}

	router := paho.NewStandardRouter()
	router.DefaultHandler(func(p *paho.Publish) { fmt.Printf("defaulthandler received message with topic: %s\n", p.Topic) })

	// a handler
	router.RegisterHandler("testtopic/#", func(p *paho.Publish) {
		fmt.Printf("testtopic/# received message with topic: %s, message : %s \n", p.Topic, string(p.Payload))
	})

	// 用户设置配置
	router.RegisterHandler("/user/+/+/online", handler.UserOnline())
	// 车辆上线
	router.RegisterHandler("/car/+/online", handler.CarOnline())
	// 用户设备离线
	router.RegisterHandler("/user/+/+/offline", handler.UserOffline())
	// 车辆下线
	router.RegisterHandler("/car/+/offline", handler.CarOffline())

	fmt.Println("RegisterMqttHandlers success")
	// serverCtx.MqttClient.Client.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
	// 	router.Route(pr.Packet.Packet())
	// 	return true, nil
	// })
	return router
}

// MqttHandler 处理Handler类
type MqttHandler struct {
	MqttClient *autopaho.ConnectionManager
	// ServerCtx  *svc.ServiceContext
}

// UserSetting 用户设置配置
func (h *MqttHandler) UserOnline() paho.MessageHandler {
	return func(p *paho.Publish) {
		params := parseTopic(p.Topic, "/user/+/+/online")
		fmt.Printf("/user/+/+/online received message with topic: %s, message : %s, params: %v \n", p.Topic, string(p.Payload), params)
		// 这里可以添加具体的业务逻辑
		userId := params["userId"]
		deviceId := params["deviceId"]
		fmt.Printf("userId: %s, deviceId: %s\n", userId, deviceId)

		if len(userId) == 0 || len(deviceId) == 0 {
			return
		}
	}
}

// UserSync 转发车辆状态
func (h *MqttHandler) UserSync() paho.MessageHandler {
	return func(p *paho.Publish) {
		params := parseTopic(p.Topic, "/user/+/+/sync")
		fmt.Printf("/user/+/+/sync received message with topic: %s, message : %s, params: %v \n", p.Topic, string(p.Payload), params)
		// 这里可以添加具体的业务逻辑

		userId := params["userId"]
		deviceId := params["deviceId"]

		fmt.Printf("userId: %s, deviceId: %s\n", userId, deviceId)
	}
}

// CarOnline 车辆上线
func (h *MqttHandler) CarOnline() paho.MessageHandler {
	return func(p *paho.Publish) {
		params := parseTopic(p.Topic, "/car/+/online")
		fmt.Printf("/car/+/online received message with topic: %s, message : %s, params: %v \n", p.Topic, string(p.Payload), params)
		// 这里可以添加具体的业务逻辑
		carId := params["carId"]
		fmt.Printf("carId: %s\n", carId)
	}
}

// UserOffline 用户设备离线
func (h *MqttHandler) UserOffline() paho.MessageHandler {
	return func(p *paho.Publish) {
		params := parseTopic(p.Topic, "/user/+/+/offline")
		fmt.Printf("/user/+/+/offline received message with topic: %s, message : %s, params: %v \n", p.Topic, string(p.Payload), params)
		// 这里可以添加具体的业务逻辑
		userId := params["userId"]
		deviceId := params["deviceId"]
		fmt.Printf("userId: %s, deviceId: %s\n", userId, deviceId)
	}
}

// CarOffline 车辆下线
func (h *MqttHandler) CarOffline() paho.MessageHandler {
	return func(p *paho.Publish) {
		params := parseTopic(p.Topic, "/car/+/offline")
		fmt.Printf("/car/+/offline received message with topic: %s, message : %s, params: %v \n", p.Topic, string(p.Payload), params)
		// 这里可以添加具体的业务逻辑
		carId := params["carId"]
		fmt.Printf("carId: %s\n", carId)
	}
}

func (h *MqttHandler) SendObject(ctx context.Context, topic string, payload interface{}, retain bool, expire uint32) error {
	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Println("json.Marshal error:", err.Error())
		return err
	}
	// 设置超时时间
	ctx, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()

	// 设置消息过期时间
	var expiryTime uint32 = 0
	if expire <= 0 || expire > uint32(time.Now().Add(24*time.Hour).Unix()) {
		expiryTime = 24 * 3600
	} else {
		if expire > uint32(time.Now().Unix()) {
			expiryTime = expire - uint32(time.Now().Unix())
		}
	}

	// _, err = h.MqttClient.Publish(ctx, &paho.Publish{
	// 	QoS:     h.ServerCtx.Config.MqttCfg.QoS,
	// 	Topic:   topic,
	// 	Retain:  retain,
	// 	Payload: data,
	// 	Properties: &paho.PublishProperties{
	// 		MessageExpiry: paho.Uint32(expire),
	// 	},
	// })

	err = h.MqttClient.PublishViaQueue(ctx, &autopaho.QueuePublish{
		Publish: &paho.Publish{
			QoS:     1,
			Topic:   topic,
			Retain:  retain,
			Payload: data,
			Properties: &paho.PublishProperties{
				MessageExpiry: paho.Uint32(expiryTime),
			},
		},
	})
	if err != nil {
		fmt.Println("MqttHandler.SendObject error:", err.Error())
		return err
	}
	return nil
}
