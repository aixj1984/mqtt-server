# race-repro

压测 / 回归工具：用**刻意收紧**的 broker 能力（小 `MaximumInflight`、小 `ReceiveMaximum`、短 `message-expiry`）去触发历史上的并发与记账问题，并用 `/stats` 的 anomaly 启发式判断是否复现。

## 目录

| 路径 | 说明 |
|------|------|
| `server/` | 带 `/stats`、`/health` 的 MQTT broker |
| `client/` | 多场景压测客户端（paho） |

## 1. 启动 Broker

在仓库根目录：

```bash
go run ./examples/race-repro/server -addr :1883 -http :18080 \
  -max-inflight 64 -receive-maximum 8 -message-expiry 5
```

或先编译再跑：

```bash
go build -o server.exe ./examples/race-repro/server
./server.exe -addr :1883 -http :18080 -max-inflight 64 -receive-maximum 8 -message-expiry 5
```

| 参数 | 默认 | 含义 |
|------|------|------|
| `-addr` | `:1883` | MQTT TCP 地址 |
| `-http` | `:18080` | HTTP 监控端口 |
| `-max-inflight` | `64` | 单客户端 Inflight 上限（越小越容易触发丢弃 / TOCTOU） |
| `-receive-maximum` | `8` | 服务端默认发送窗口 |
| `-message-expiry` | `5` | 消息最大存活秒数（给 `expiry` 场景用） |

查看实时状态：

```bash
curl http://127.0.0.1:18080/stats
```

Broker 日志里也会周期性打印 `messages_received / inflight / inflight_dropped / anomalies`。

> **建议**：换场景前重启 broker，避免共享订阅残留会话、累计计数干扰判断。

## 2. 运行 Client

另开终端（仓库根目录）：

```bash
go run ./examples/race-repro/client -scenario <name> [选项...]
```

或：

```bash
go build -o client.exe ./examples/race-repro/client
./client.exe -scenario fanin -publishers 20 -rate 800 -duration 45s -slow-ms 20
```

### 公共参数

| 参数 | 默认 | 含义 |
|------|------|------|
| `-broker` | `tcp://127.0.0.1:1883` | Broker URL |
| `-stats` | `http://127.0.0.1:18080/stats` | `/stats` 地址 |
| `-scenario` | `fanin` | 见下方场景表 |
| `-publishers` | `20` | 并发发布端数量 |
| `-rate` | `500` | 总体发布速率（msg/s，分摊到各 publisher） |
| `-duration` | `30s` | 场景时长 |
| `-slow-ms` | `10` | 订阅端 ACK 前睡眠（ms），`0` = 立刻 ACK |
| `-receive-maximum` | `8` | 订阅端 MQTT5 ReceiveMaximum |
| `-topic` | `race/demo/data` | 普通主题 |
| `-share-topic` | `$share/race-group/race/demo/data` | 共享订阅过滤器 |

## 3. 场景说明与推荐命令

| 场景 | 针对的历史问题 | 推荐命令 |
|------|----------------|----------|
| `fanin` | 多发布 → 单慢消费者；`MaximumInflight` Len-then-Reserve TOCTOU | `./client.exe -scenario fanin -publishers 20 -rate 800 -duration 45s -slow-ms 20` |
| `packetid-clash` | 同连接既收又发，inbound PacketID 冲掉 outbound Inflight | `./client.exe -scenario packetid-clash -publishers 16 -rate 600 -duration 30s -slow-ms 10` |
| `session-takeover` | 同 ClientID 重连：`Clone` + 配额重置 | `./client.exe -scenario session-takeover -publishers 10 -rate 400 -duration 20s` |
| `expiry` | Inflight / deferred 过期清理时配额未恢复 | `./client.exe -scenario expiry -publishers 8 -rate 200 -duration 15s`（broker 需 `-message-expiry 5`） |
| `all` | 先 `fanin` 再 `packetid-clash` 冒烟 | `./client.exe -scenario all -publishers 10 -rate 400 -duration 20s` |

Windows 示例（与上面等价）：

```powershell
# 终端 1
.\server.exe -addr :1883 -http :18080 -max-inflight 64 -receive-maximum 8 -message-expiry 5

# 终端 2
.\client.exe -scenario packetid-clash -publishers 16 -rate 600 -duration 30s
```

## 4. 如何读 VERDICT

客户端结束时会拉一次 `/stats`（相对本场景开始的 **delta**），再打印判定：

| VERDICT | 含义 |
|---------|------|
| `REPRODUCED … anomaly_count>0` | Broker 启发式命中（配额泄漏 / Inflight 记账异常等）→ **真复现** |
| `REPRODUCED … subscriber stall` | 收到很多但 ACK 很少 → 订阅端卡住 |
| `NOT reproduced … rate-limited drops` | 本场景 `Δinflight_dropped` 能解释 gap → **限流丢弃**，不是目标竞态 |
| `inconclusive … received=0` | 订阅端没收到任何消息（主题 / 共享组残留 / broker 未起） |
| `inconclusive … gap not explained` | 有 gap，但丢弃和 anomaly 都解释不清 → 再试或查 `/stats` |
| `no clear bug` | 本轮未看到异常 |

注意：

- **不要**只看 `recv_ratio` / `gap`。`-max-inflight 64` 高压下 gap 很大是常态。
- `anomaly_count=0` 且 `received ≈ acked` → 一般说明旧的 clash / quota 死锁指纹**未复现**。
- `inflight` 全局计数若短暂非 0，看 `/stats` 里各 client 的 `inflight_len` / `send_quota`；`clients=0` 后应变回 `0`。

### `/stats` 里常见字段

- `anomaly_count` / `clients[].anomaly`：记账异常
- `inflight_dropped`：触顶 Inflight 后主动丢弃（限流）
- `clients[].send_quota` / `deferred` / `inflight_len`：发送窗口与 deferred 积压

## 5. 单元测试（库内回归）

不依赖 race-repro 进程，在仓库根目录：

```bash
go test -count=1 -timeout 60s \
  -run "TestSessionTakeoverPreservesInflightGauge|TestInboundPublishDoesNotDeleteOutbound|TestClearExpired|TestResumeDeferred|TestPacketID" \
  .
```

或跑全量：

```bash
go test -count=1 -timeout 120s .
```
