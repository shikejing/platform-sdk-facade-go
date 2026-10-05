# platform-sdk-facade-go

面向多个后端平台服务的统一客户端门面。账户、匹配、房间、榜单等能力收敛到
`facade.Client` 一组接口，各后端的语义差异由 `backends` 包中的适配器吸收。

## 包结构

| 包 | 说明 |
| --- | --- |
| `facade` | 门面核心：凭据续期、健康熔断与切换、幂等写队列与重放、限流配额、归一化模型、审计事件 |
| `backends` | 各平台参考适配器（PlatformA / PlatformB），演示如何吸收后端语义差异 |

## 快速开始

```go
client := facade.NewClient(facade.Config{
    Backends: []facade.BackendConfig{ // 按优先级排序
        {Backend: backends.NewPlatformA("a", transportA),
         CredMargin: 30 * time.Second,
         Health:    facade.HealthConfig{FailureThreshold: 3, OpenCooldown: 30 * time.Second, SuccessThreshold: 2},
         RateLimit: facade.RateLimitConfig{Rate: 100, Burst: 20, Quota: 10000, QuotaWindow: time.Hour}},
        {Backend: backends.NewPlatformB("b", transportB)},
    },
    RequestTimeout: 5 * time.Second,
    Sink:           facade.NewSlogSink(nil), // 审计事件出口
})

ctx := facade.WithTraceID(context.Background(), "req-123")
info, err := client.GetAccount(ctx, "u1")
```

## 能力说明

### 1. 统一凭据续期
- 各后端凭据形式不同（A：`token`+`expires_in`；B：`session_key`+`expires_at`），
  适配器统一映射为 `facade.Credential{Token, ExpiresAt}`。
- `CredentialManager` 保证并发下同一凭据只续期一次（singleflight），
  等待中的请求共享续期结果。
- 续期失败返回 `*facade.Error`，`facade.IsCode(err, facade.ReasonCredentialRenewFailed)` 可区分。
- 凭据按 `CredMargin` 预留安全余量，过期或即将过期的凭据绝不会被用于发请求。

### 2. 后端故障切换
- 每个后端有独立 `healthTracker`，状态：`healthy → degraded → circuit_open → half_open → healthy`。
- 连续失败 `FailureThreshold` 次熔断；熔断的后端不会被选中。
- 冷却 `OpenCooldown` 后进入半开放行探测；需连续成功 `SuccessThreshold` 次才回切，
  半开期间任何失败立即重新熔断，避免健康度抖动时反复横跳。
- 超时归类为 `backend_timeout`，其他失败归类为 `backend_unavailable`。
- 切换（`failover`）与回切（`failback`）均记录审计事件并注明原因与目标后端。

### 3. 幂等写入与重放
- 写操作必须携带幂等键 `WriteOp.ID`；重复提交返回 `ReasonWriteDeduplicated`，不产生重复副作用。
- 后端不可用时写入进入本地队列，`SubmitWrite` 返回 `ReasonWriteQueued`（可区分，非失败）。
- `ReplayWrites` 按入队顺序重放；中途失败返回 `ReasonReplayInterrupted`，
  已完成进度保留，剩余操作保持原顺序，不跳号、不重复。

### 4. 限流与配额隔离
- 每个后端独享令牌桶（`Rate`/`Burst`）与配额窗口（`Quota`/`QuotaWindow`），状态完全隔离。
- `Wait=false`：超限立即返回 `ReasonRateLimited`/`ReasonQuotaExceeded`，并携带 `RetryAfter` 退避提示；
  门面随后尝试下一个后端。
- `Wait=true`：超限请求排队等待令牌，可被 context 取消。
- 限流不计入健康失败，不会触发熔断。

### 5. 结果归一化
- 对外模型使用 `facade.Field[T]`，字段状态三选一：
  `FieldOK`（正常映射）、`FieldMissing`（后端未返回）、`FieldUnmapped`（类型无法映射）。
- 缺失/无法映射的字段保持零值并显式标记，绝不用默认值掩盖。

### 6. 审计与可观测
- 所有续期、熔断、切换、回切、重放、限流事件通过 `Sink` 输出结构化 `Event`，
  携带稳定原因码（`ReasonCode`）与 `TraceID`。
- 内置 `SlogSink`（slog 结构化日志）与 `MemorySink`（测试用）。
- 用 `facade.WithTraceID` 注入 trace id，即可还原一次请求的完整链路。

## 后端语义差异对照

| 维度 | PlatformA | PlatformB |
| --- | --- | --- |
| 凭据字段 | `token` + `expires_in`（相对秒） | `session_key` + `expires_at`（unix 秒） |
| 账户 | `account_id`/`nickname`/`region`/`level`(数值) | `uid`/`display_name`/`lvl`(字符串)；无 `region` → `FieldMissing` |
| 匹配票据 | `ticket_id`/`status`/`eta_ms`（毫秒） | `ticket`/`state`/`eta_seconds`（秒） |
| 房间 | `room_id`/`host`/`capacity`/`members` | `roomId`/`owner`/`max`/`cur` |
| 榜单 | `entries:[{rank,account_id,score}]` | `items:[{position,player,points}]` |
| 写幂等键 | `idempotency_key` | `request_id` |

## 配置项

| 配置 | 说明 | 默认 |
| --- | --- | --- |
| `BackendConfig.CredMargin` | 凭据过期安全余量 | 0 |
| `HealthConfig.FailureThreshold` | 熔断所需连续失败次数 | 3 |
| `HealthConfig.OpenCooldown` | 熔断到半开的冷却时长 | 30s |
| `HealthConfig.SuccessThreshold` | 回切所需连续成功次数 | 2 |
| `RateLimitConfig.Rate`/`Burst` | 令牌桶速率/容量，0 表示不限 | 不限 |
| `RateLimitConfig.Quota`/`QuotaWindow` | 配额窗口上限/长度，0 表示不限 | 不限 |
| `RateLimitConfig.Wait` | 超限排队（true）或立即退避（false） | false |
| `Config.RequestTimeout` | 单次后端调用超时 | 5s |
| `Config.Sink` | 审计事件出口 | 无（丢弃） |

## 测试

```sh
go test ./... -race
```

覆盖：并发续期（单次续期/失败共享/过期凭据拒发）、故障切换与带回切迟滞的回切、
重放去重与中断恢复、限流配额的后端隔离、字段归一化（OK/Missing/Unmapped）。
