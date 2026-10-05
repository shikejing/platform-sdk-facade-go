# platform-sdk-facade-go

面向多个后端平台服务的统一客户端封装。账户、匹配、房间与榜单等能力收敛到
`facade.Client` 一组内部接口，由本封装统一吸收各后端在凭据形式、字段命名、
限流策略上的语义差异，并提供会话续期、故障切换、失败重放、限流隔离、
结果归一化与审计能力。

## 结构

| 文件 | 职责 |
| --- | --- |
| `facade/types.go` | 统一操作类型（`OpKind`）、`Request`、可区分错误 |
| `facade/credentials.go` | 凭据续期（single-flight，结果共享，过期拒发） |
| `facade/health.go` | 健康状态机：熔断 / 半开 / 回切（防抖动） |
| `facade/ratelimit.go` | 每后端独立令牌桶限流（backoff / queue 两种超限策略） |
| `facade/replay.go` | 本地写队列：幂等去重、按序重放、中断保留进度 |
| `facade/normalize.go` | 字段归一化，缺失/未映射显式标记 |
| `facade/audit.go` | 结构化事件与稳定原因码 |
| `facade/client.go` | 编排：限流 → 选后端 → 取凭据 → 调用 → 归一化 |
| `backends/` | 两个语义不同的模拟后端适配器（alpha / beta） |

## 各后端语义差异

| 维度 | alpha（主） | beta（备） |
| --- | --- | --- |
| 凭据形式 | `alpha-token-N`，60s 过期 | `beta:sig:N`，5min 过期 |
| 账户字段 | `account_id/display_name/region/created_ts` | `uid/nick/signupTime`，**不返回 region** |
| 榜单字段 | `player/points/position` | `uid/score`，**无 rank 概念** |
| 匹配状态 | `state=open` | `phase=WAITING` |

差异由适配器吸收：凭据差异由各自的 `CredentialSource` 处理；字段差异由
各后端的 `FieldMapping` 归一化到统一字段（`account_id/name/region/...`）。
无法映射的字段标记为 `FieldUnmapped`，后端未返回的标记为 `FieldMissing`，
`NormalizedResult.Get` 对二者返回 `ok=false`，**不会用默认值掩盖**。

## 关键行为

### 统一凭据续期
- 同一后端并发请求下续期只真正发生一次（single-flight），等待者共享结果
  （审计事件 `RENEW_SHARED`）。
- 续期失败返回 `*facade.RenewalError`（可 `errors.As` 区分）；签发即过期
  （落入安全余量 `RenewMargin`）的凭据视同续期失败，绝不向外发放。
- 发送前再次校验凭据未过期；后端 401 类错误可 `Invalidate()` 强制续期。

### 故障切换与回切
- 每个后端一个 `HealthTracker`：`healthy → open → half_open → healthy`。
- 连续失败达 `FailureThreshold` 熔断；熔断期间 `pick` 不再选中该后端，
  请求按注册顺序切换到备用后端（事件 `FAILOVER`，含原因）。
- `OpenDuration` 后进入半开放探；连续成功达 `SuccessThreshold` 才回切
  （事件 `FAILBACK`），中途失败立即重新熔断，避免健康度抖动时反复横跳。

### 幂等写入与重放
- 所有后端不可用时写请求进入本地队列（返回 `ErrQueued`）；同一
  `IdempotencyKey` 重复提交返回 `ErrDuplicate`，不产生重复副作用。
- `ReplayQueue` 按入队顺序重放；中途失败即停，保留该条及后续条目，
  不跳号、不重复；已应用键永久去重（`IsApplied`）。

### 限流与配额隔离
- 每个后端独立的令牌桶（`RatePerSec`/`Burst`），互不影响。
- 超限行为按后端策略：`OverLimitBackoff` 立即返回 `ErrRateLimited`；
  `OverLimitQueue` 排队等待至多 `QueueWait`。

### 审计与可观测
- 所有续期、切换、回切、重放、限流、字段缺失事件写入 `facade.Logger`，
  带稳定原因码（`ReasonCode`）与 `TraceID`，可据此还原一次请求的完整链路
  （`REQUEST_START → … → REQUEST_SUCCESS/REQUEST_FAILURE`）。

## 配置项（`BackendOptions`）

| 字段 | 说明 | 默认 |
| --- | --- | --- |
| `Health.FailureThreshold` | 连续失败熔断阈值 | 3 |
| `Health.OpenDuration` | 熔断保持时长 | 10s |
| `Health.SuccessThreshold` | 回切所需连续成功数 | 2 |
| `Rate.RatePerSec` / `Rate.Burst` | 令牌桶速率 / 容量 | 50 / 10 |
| `Rate.OverLimit` | `backoff` 或 `queue` | `backoff` |
| `Rate.QueueWait` | queue 模式最长等待 | 2s |
| `RenewMargin` | 凭据过期安全余量（提前续期） | 0 |

## 测试

```sh
go test ./... -race
```

覆盖：并发续期 single-flight 与错误区分、熔断/切换/回切防抖、重放去重与
中断恢复、后端限流隔离、字段归一化标记、审计链路 TraceID 串联。
