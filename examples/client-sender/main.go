package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	uuid "github.com/google/uuid"
)

// ──────────────────────────────────────────────
// Config
// ──────────────────────────────────────────────

type MQTTConfig struct {
	Endpoint  string
	ClientID  string
	Username  string
	Password  string
	CarUUID   string
	KeepAlive uint16
}

// ──────────────────────────────────────────────
// Payload structs
// ──────────────────────────────────────────────

type Operator struct {
	CarType string `json:"carType"`
	Name    string `json:"name"`
	OptID   string `json:"optId"`
	Role    string `json:"role"`
}

type LocationProp struct {
	PropertyName string `json:"propertyName"`
	AreaType     string `json:"areaType"`
	AreaName     string `json:"areaName"`
	Type         string `json:"type"`
	Value        string `json:"value"`
}

type Settings struct {
	CarLocation []LocationProp `json:"CAR_LOCATION"`
}

type Source struct {
	DeviceID string `json:"deviceId"`
	Type     string `json:"type"`
}

type SyncPayloadInner struct {
	BusinessType string   `json:"businessType"`
	Operator     Operator `json:"operator"`
	PropVer      int      `json:"propVer"`
	Settings     Settings `json:"settings"`
	Source       Source   `json:"source"`
}

type SyncMessage struct {
	Action    int              `json:"action"`
	MsgID     string           `json:"msgId"`
	Payload   SyncPayloadInner `json:"payload"`
	Timestamp int64            `json:"timestamp"`
}

func buildSyncMessage(lat string) SyncMessage {
	now := time.Now().UnixMilli()
	return SyncMessage{
		Action: 2,
		MsgID:  uuid.New().String(),
		Payload: SyncPayloadInner{
			BusinessType: "GoldenCar",
			Operator: Operator{
				CarType: "",
				Name:    "96c4de34",
				OptID:   "96c4de34",
				Role:    "96c4de34",
			},
			PropVer: 1,
			Settings: Settings{
				CarLocation: []LocationProp{
					{
						PropertyName: "CAR_LOCATION_LAT",
						AreaType:     "",
						AreaName:     "",
						Type:         "String",
						Value:        lat,
					},
				},
			},
			Source: Source{DeviceID: "96c4de34", Type: "MQTT"},
		},
		Timestamp: now,
	}
}

// ──────────────────────────────────────────────
// SendResult
// ──────────────────────────────────────────────

type SendResult struct {
	Total   int
	Success int64
	Failed  int64
	Elapsed time.Duration
}

func (r SendResult) Print() {
	rate := float64(r.Total) / r.Elapsed.Seconds()
	fmt.Printf("\n── 发送统计 ───────────────────────────────\n")
	fmt.Printf("  总计: %d  ✓ 成功: %d  ✗ 失败: %d\n", r.Total, r.Success, r.Failed)
	fmt.Printf("  耗时: %s  平均: %.1f 条/s\n", r.Elapsed.Round(time.Millisecond), rate)
	fmt.Println("────────────────────────────────────────────")
}

// ──────────────────────────────────────────────
// CarMQTTClient
// ──────────────────────────────────────────────

type CarMQTTClient struct {
	cfg   MQTTConfig
	cm    *autopaho.ConnectionManager
	topic string
}

func NewCarMQTTClient(cfg MQTTConfig) *CarMQTTClient {
	return &CarMQTTClient{
		cfg:   cfg,
		topic: fmt.Sprintf("/car/%s/sync", cfg.CarUUID),
	}
}

func (c *CarMQTTClient) Connect(ctx context.Context) error {
	u, err := url.Parse(c.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("解析 Endpoint 失败: %w", err)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     c.cfg.KeepAlive,
		CleanStartOnInitialConnection: true,
		ConnectRetryDelay:             5 * time.Second,
		OnConnectionUp: func(_ *autopaho.ConnectionManager, _ *paho.Connack) {
			fmt.Println("✓ MQTT 连接成功!")
		},
		OnConnectError: func(err error) {
			fmt.Printf("✗ MQTT 连接错误: %v\n", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: c.cfg.ClientID,
			OnClientError: func(err error) {
				fmt.Printf("✗ MQTT 客户端错误: %v\n", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				fmt.Printf("- MQTT 服务端主动断开: reason=%d\n", d.ReasonCode)
			},
		},
		ConnectPacketBuilder: func(cp *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			cp.Username = c.cfg.Username
			cp.Password = []byte(c.cfg.Password)
			cp.UsernameFlag = true
			cp.PasswordFlag = true
			return cp, nil
		},
	}

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return err
	}
	if err := cm.AwaitConnection(ctx); err != nil {
		return err
	}
	c.cm = cm
	return nil
}

func (c *CarMQTTClient) Disconnect(ctx context.Context) {
	if c.cm != nil {
		_ = c.cm.Disconnect(ctx)
	}
	fmt.Println("- 已断开 MQTT 连接")
}

// buildPayload 每次调用都会生成唯一 MsgID
func (c *CarMQTTClient) buildPayload(lat string) ([]byte, error) {
	return json.Marshal(buildSyncMessage(lat))
}

// ──────────────────────────────────────────────
// SendConcurrent
// ──────────────────────────────────────────────

// SendConcurrent 使用 paho.golang（MQTT 5.0）并发发送消息。
// paho.golang 的 Publish 是同步的：QoS1 等待 PUBACK 后返回，QoS0 直接返回，
// 因此无需原来的两阶段 token 收集模式。
func (c *CarMQTTClient) SendConcurrent(
	ctx context.Context,
	lat string,
	count, workers int,
	interval time.Duration,
	qos byte,
	verbose bool,
) SendResult {
	if workers <= 0 {
		workers = 1
	}
	if workers > count {
		workers = count
	}

	var (
		successN int64
		failedN  int64
	)

	jobs := make(chan int, count)
	for i := 1; i <= count; i++ {
		jobs <- i
	}
	close(jobs)

	var progressDone chan struct{}
	if !verbose {
		progressDone = make(chan struct{})
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					s := atomic.LoadInt64(&successN)
					f := atomic.LoadInt64(&failedN)
					fmt.Printf("\r  进度: %d/%d  ✓ %d ✗ %d   ",
						s+f, count, s, f)
				case <-progressDone:
					return
				}
			}
		}()
	}

	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lastSent time.Time
			for idx := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if interval > 0 {
					if wait := interval - time.Since(lastSent); wait > 0 {
						select {
						case <-time.After(wait):
						case <-ctx.Done():
							return
						}
					}
				}

				data, err := c.buildPayload(lat)
				if err != nil {
					fmt.Printf("✗ [%d/%d] JSON 序列化失败: %v\n", idx, count, err)
					atomic.AddInt64(&failedN, 1)
					continue
				}

				_, err = c.cm.Publish(ctx, &paho.Publish{
					Topic:   c.topic,
					QoS:     qos,
					Retain:  false,
					Payload: data,
				})
				lastSent = time.Now()

				if err != nil {
					if verbose {
						fmt.Printf("✗ [%d/%d] 发布失败: %v\n", idx, count, err)
					}
					atomic.AddInt64(&failedN, 1)
				} else {
					if verbose {
						fmt.Printf("  ✓ [%d/%d] 已发送 LAT=%s\n", idx, count, lat)
					}
					atomic.AddInt64(&successN, 1)
				}
			}
		}()
	}

	wg.Wait()

	if !verbose {
		close(progressDone)
		fmt.Println()
	}

	return SendResult{
		Total:   count,
		Success: atomic.LoadInt64(&successN),
		Failed:  atomic.LoadInt64(&failedN),
		Elapsed: time.Since(start),
	}
}

// ──────────────────────────────────────────────
// 交互式控制台
// ──────────────────────────────────────────────

func interactiveShell(ctx context.Context, c *CarMQTTClient) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-sigCh
		fmt.Println("\n收到中断信号，取消发送...")
		cancel()
	}()

	fmt.Println()
	fmt.Println(strings.Repeat("=", 72))
	fmt.Println("sync 发送控制台  (MQTT 5.0 · per-worker 独立限速 · 唯一 MsgID)")
	fmt.Println(strings.Repeat("=", 72))
	fmt.Println("命令格式:")
	fmt.Println("  send <lat>                                  发 1 条")
	fmt.Println("  send <lat> <N>                              顺序发 N 条（默认间隔 0.5s）")
	fmt.Println("  send <lat> <N> <间隔s>                      N 条，每 worker 独立限速（0=不限）")
	fmt.Println("  send <lat> <N> <间隔s> <并发数>             N 条，多 worker 并发")
	fmt.Println("  send <lat> <N> <间隔s> <并发数> <qos>       qos=0 极速/qos=1 确认（默认1）")
	fmt.Println("  send <lat> <N> <间隔s> <并发数> <qos> -v    详细模式")
	fmt.Println("  quit                                        退出")
	fmt.Println()
	fmt.Println("示例（10并发 不限速 QoS0 极速发1000条）:")
	fmt.Println("  send 39.9042 1000 0 10 0")
	fmt.Println(strings.Repeat("=", 72))
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">>> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "quit", "exit":
			fmt.Println("正在退出...")
			cancel()
			return

		case "send":
			if len(parts) < 2 {
				fmt.Println("✗ 用法: send <lat> [N] [间隔s] [并发数] [qos] [-v]")
				continue
			}

			lat := parts[1]

			count := 1
			if len(parts) >= 3 {
				v, err := strconv.Atoi(parts[2])
				if err != nil || v <= 0 {
					fmt.Println("✗ 条数必须是正整数")
					continue
				}
				count = v
			}

			interval := 500 * time.Millisecond
			if len(parts) >= 4 {
				v, err := strconv.ParseFloat(parts[3], 64)
				if err != nil || v < 0 {
					fmt.Println("✗ 间隔必须是非负数（秒），0 表示不限速")
					continue
				}
				interval = time.Duration(v * float64(time.Second))
			}

			workers := 1
			if len(parts) >= 5 && parts[4] != "-v" {
				v, err := strconv.Atoi(parts[4])
				if err != nil || v <= 0 {
					fmt.Println("✗ 并发数必须是正整数")
					continue
				}
				workers = v
			}

			var qos byte = 1
			if len(parts) >= 6 && parts[5] != "-v" {
				v, err := strconv.Atoi(parts[5])
				if err != nil || (v != 0 && v != 1) {
					fmt.Println("✗ qos 只能是 0 或 1")
					continue
				}
				qos = byte(v)
			}

			verbose := false
			for _, p := range parts {
				if p == "-v" {
					verbose = true
					break
				}
			}

			limitStr := "不限速"
			if interval > 0 {
				limitStr = fmt.Sprintf("%.3fs/条(per worker)", interval.Seconds())
			}
			workerThroughput := "∞"
			if interval > 0 {
				wt := float64(workers) / interval.Seconds()
				workerThroughput = fmt.Sprintf("≈%.0f条/s", wt)
			}
			fmt.Printf("开始发送 → LAT=%s  条数=%d  限速=%s  并发=%d  QoS=%d  预期吞吐=%s  详细=%v\n",
				lat, count, limitStr, workers, qos, workerThroughput, verbose)

			sendCtx, sendCancel := context.WithCancel(ctx)
			result := c.SendConcurrent(sendCtx, lat, count, workers, interval, qos, verbose)
			sendCancel()
			result.Print()

		default:
			fmt.Printf("✗ 未知命令: %s\n", cmd)
		}
	}
}

// ──────────────────────────────────────────────
// main
// ──────────────────────────────────────────────

func main() {
	cfg := MQTTConfig{
		Endpoint:  "tcp://127.0.0.1:1883",
		ClientID:  "admin-api",
		Username:  "admin",
		Password:  "adminpass",
		CarUUID:   "carUUIDMOCK",
		KeepAlive: 60,
	}

	fmt.Printf("MQTT Broker : %s\n", cfg.Endpoint)
	fmt.Printf("ClientID    : %s\n", cfg.ClientID)
	fmt.Printf("Topic       : /car/%s/sync\n", cfg.CarUUID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewCarMQTTClient(cfg)
	if err := client.Connect(ctx); err != nil {
		fmt.Printf("✗ 连接失败: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer disconnectCancel()
		client.Disconnect(disconnectCtx)
	}()

	interactiveShell(ctx, client)
	fmt.Println("程序已退出")
}
