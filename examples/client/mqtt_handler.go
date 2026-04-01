package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/packets"
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

// 全局车辆锁映射
var G_CarLocks = &sync.Map{}

// 预编译正则表达式，避免每次调用都编译
var carSyncRegex = regexp.MustCompile(`^/car/([^/]+)/online$`)

// GetSetupFunc 获取 MQTT 连接设置函数
func GetSetupFunc() func(*autopaho.ConnectionManager, *paho.Connack) {
	return func(cm *autopaho.ConnectionManager, connack *paho.Connack) {
		// 添加消息处理器
		router := RegisterMqttHandlers(cm)
		cm.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
			// 某些topic需要串行处理，例如：/user/+/+/setting, /car/+/sync
			// 使用预编译的正则表达式匹配
			matches := carSyncRegex.FindStringSubmatch(pr.Packet.Topic)
			if len(matches) > 1 {
				carId := matches[1]
				// 获取或创建该车辆的处理通道
				ch, exists := G_CarLocks.Load(carId)
				if !exists {
					newCh := make(chan *packets.Publish, 1024)
					G_CarLocks.Store(carId, newCh)
					ch = newCh
					// 启动该车辆的专属顺序处理器
					go func(cid string, messageChan chan *packets.Publish) {
						for packet := range messageChan {
							router.Route(packet)
						}
					}(carId, newCh)
				}

				// 发送消息到处理通道
				select {
				case ch.(chan *packets.Publish) <- pr.Packet.Packet():
					// 成功
				default:
					fmt.Printf("车辆 %s 处理器繁忙，丢弃消息,topic %s", carId, pr.Packet.Topic)
				}
			} else {
				// 其他topic可以异步处理
				go router.Route(pr.Packet.Packet())
			}
			return true, nil
		})

		// 重新建立订阅
		cm.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{
				{Topic: "/user/+/+/online", QoS: 1, NoLocal: true},
				{Topic: "/user/+/+/offline", QoS: 1, NoLocal: true},
				{Topic: "/car/+/online", QoS: 1, NoLocal: true},
				{Topic: "/car/+/offline", QoS: 1, NoLocal: true},
				{Topic: "/car/+/sync", QoS: 1, NoLocal: true},
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
		mqttClient.PublishViaQueue(context.Background(), &autopaho.QueuePublish{
			Publish: &paho.Publish{
				QoS:     1,
				Topic:   "testtopic/response",
				Retain:  false,
				Payload: []byte("response to " + string(p.Payload)),
			},
		})
	})

	// 用户设置配置
	router.RegisterHandler("/user/+/+/online", handler.UserOnline())
	// 车辆上线
	router.RegisterHandler("/car/+/online", handler.CarOnline())
	// 用户设备离线
	router.RegisterHandler("/user/+/+/offline", handler.UserOffline())
	// 车辆下线
	router.RegisterHandler("/car/+/offline", handler.CarOffline())

	router.RegisterHandler("/car/+/sync", handler.CarSync())

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
		time.Sleep(10 * time.Millisecond)
		// carId := params["carId"]
		// fmt.Printf("carId: %s\n", carId)
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
		time.Sleep(10 * time.Millisecond)
		// carId := params["carId"]
		// fmt.Printf("carId: %s\n", carId)
	}
}

func (h *MqttHandler) CarSync() paho.MessageHandler {
	return func(p *paho.Publish) {
		params := parseTopic(p.Topic, "/car/+/sync")
		fmt.Printf("/car/+/sync received message with topic: %s, message : %s, params: %v \n", p.Topic, string(p.Payload), params)
		// 这里可以添加具体的业务逻辑
		time.Sleep(10 * time.Millisecond)
		// carId := params["carId"]
		// fmt.Printf("carId: %s\n", carId)
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
