## Context

当前分层是 UI → 应用桥 → `lingo-core::Service`（同步、SQLite）→ 无网络；`lingo-net` 只有端点、mDNS、认证、帧协议的孤立原语。本次只做局域网直连的端到端传输，先把主链路打通；会合/中继和链接码配对留到下一变更。

## Goals / Non-Goals

- Goals：同一局域网内两台 Mac 可发现、认证、发送、接收、暂停/恢复/取消、断点续传；失败可解释；路径安全；重启可恢复。
- Non-Goals：跨网/中继、链接码配对、配额与推送、系统通知、空目录与文件元数据保留、同名覆盖。

## Decisions

### 1. 网络层归属与进程模型

- 传输逻辑放在 `lingo-net`，`lingo-core` 不依赖 QUIC。
- `lingo-net::agent::TransferAgent` 拥有端点、发现、连接与传输任务；通过 `AgentCommand` / `AgentEvent` 与 app 通信。
- app 的 core 线程改为单个 tokio 运行时，`tokio::select!` 同时处理 UI 命令和 agent 事件；`Service` 仍在该任务内顺序处理并落库。
- UI 仍只在 Slint 主线程更新，沿用现有 80 ms 事件泵和进度合并。

### 2. 会话与认证

- 端点绑定 `0.0.0.0:0`（测试可注入回环地址），启动后把实际端口写入 `AppState.port` 并用于 mDNS advertisement。
- 握手在控制流上完成：

```text
C -> S  ClientHello { v, did_c, nonce_c }
S -> C  ServerHello { v, did_s, nonce_s, proof_s }
C -> S  ClientAuth  { proof_c }
S -> C  SessionReady { v }
```

- 签名载荷固定为 `"lingo-auth-v1" || exporter || did_c || did_s || nonce_c || nonce_s`，`exporter` 来自 `quinn::Connection::export_keying_material`（32 字节）。双方各自生成挑战，解决重放；exporter 绑定解决跨连接中继。
- 认证完成前只接受握手帧，单帧上限 64 KiB；认证后控制帧上限 1 MiB。
- 每个 DID 同时只保留一个会话；连接认证出的 DID 必须与发现时预期的 DID 一致，否则拒绝。

### 3. 传输标识与状态机

- 本地面向 UI 的消息继续用数据库 `id`；所有对端消息改用 `tid: Uuid`，避免双端自增 id 不一致。
- `TransferStatus` 扩展为 `Pending | Ready | Paused | Complete | Canceled | Failed`：
  - `Pending`：已发出 offer 等待接收方确认，或收到 offer 等待用户确认；
  - `Ready`：已接受并允许传数据；
  - `Failed`：保留 `err` 供 UI 展示。
- 状态合并优先级：`Failed` > `Canceled` > `Complete(双方)` > `Ready` > `Paused`。修正现有 `effective_status` 中“本地 Ready 掩盖对端 Canceled”的问题。
- 接受/拒绝/暂停/恢复/取消/完成/失败都由控制流广播，双端各自落库。

### 4. 控制协议与清单传输

- 清单分块发送，避免当前 1 MiB 行上限与 100,000 条 manifest 上限冲突：
  - `TransferOffer { tid, note, syn, entry_count, total_bytes }`
  - `TransferOfferChunk { tid, entries }`（每块 256 条）
  - `TransferOfferDone { tid }`
  - `TransferAccept { tid }` / `TransferDecline { tid, reason }`
  - `TransferControl { tid, Pause|Resume|Cancel }`
  - `TransferComplete { tid }` / `TransferFailed { tid, err }`
- 接收方先完整校验 manifest（相对路径、控制字符、`..`、长度、重复路径、条目数），任一非法则拒绝整个 offer，不做部分接收。
- 接收方接受后才确定并持久化每个文件的最终路径；目标目录由用户选择。

### 5. 数据面

- 每个文件一条双向 QUIC stream，单传输最多 4 个文件并发；控制流独立。
- 文件流协议：`FileStart { tid, index, path, size, mtime, offset }` → `FileAccept { offset }` → 分块字节 → `FileEnd { sha256 }` → `FileDone { ok, err }`。
- 分块大小 256 KiB；写入使用有界缓冲，进度按 200 ms 采样合并，禁止逐块刷新 UI。
- 完整性：发送方与接收方各自滚动计算 SHA-256，接收方在 `FileEnd` 后比对最终哈希；不一致则标记该文件失败并终止传输。
- 写入：临时文件放在目标根目录下的 `.lingo-part/<tid>/`，完成后 `sync_data`，再原子重命名到接受时确定的最终路径；绝不覆盖已有文件，冲突用 `unique_path` 避让。
- 路径安全：最终路径必须经过 `validate_manifest_path` / `destination_path` / `ensure_within_root`；目标根必须 canonicalize；父目录不得通过符号链接逃逸。
- 取消：中止流、删除临时目录；已完成的文件保留。失败：保留临时文件以便重试，但 UI 明确标记。

### 6. 断点续传与恢复

- 接收方是偏移权威：重连后发送 `TransferResumeQuery { tid }`，接收方返回每个文件的 `offset` 与状态；发送方从该偏移继续。
- 发送方通过 `path_map` 把 manifest 顶层名映射回本地源路径；续传前校验源文件 `size`/`mtime`，变化则从 0 重传并提示。
- 接收方续传前对临时文件前缀重新计算 SHA-256（发送方同样重算源前缀），保证最终哈希覆盖完整文件。
- `transfer_files` 持久化 `path`、`final_path`、`size`、`offset`、`state`、`sha256`。偏移每 1 s 或 4 MiB 落库一次；启动时以临时文件实际长度为准修正。
- 启动恢复：`Ready` → `Paused`；源文件缺失 → `Failed`；临时文件存在则保留待恢复；最终文件已存在且临时文件存在时，删除临时文件并标记完成。
- 暂存（staging）持久化不在本期，转让记录保留 `manifest`/`path_map` 以便重试。

### 7. 错误与限制

- 明确错误：`PeerOffline`、`AuthFailed`、`OfferRejected`、`SourceChanged`、`SourceMissing`、`InvalidPath`、`DiskError`、`HashMismatch`、`Canceled`、`Timeout`。
- 限制：manifest ≤ 100,000 条、单路径 ≤ 4096 字节、单传输并发文件 ≤ 4、控制帧 ≤ 1 MiB、握手帧 ≤ 64 KiB。
- 超时：握手 10 s、offer 响应 60 s、数据流空闲 30 s；超时按 `Failed` 处理并通知对端。

## Risks / Trade-offs

- macOS 本地网络权限会导致“发现不到设备”：CI 只跑回环/直连测试，真机验收必须打包并声明 `NSLocalNetworkUsageDescription`、`NSBonjourServices`。
- 续传前缀重算哈希会增加一次读盘；换来不依赖持久化哈希状态、可验证完整文件。
- 每文件一条流增加连接内流数量；v1 限制 4 并发，后续可按链路质量调整。
- 清单分块增加协议复杂度；避免任意提高帧上限带来的内存 DoS。
- 不使用链接码时，首次入站依赖用户确认；发现的设备名不可信，UI 只能作为展示，认证 DID 才是身份。

## Migration Plan

- `transfer_files` 使用 `CREATE TABLE IF NOT EXISTS` 新增，旧库无需重建。
- `TransferStatus` 新增字符串值；旧数据解析保持不变，未知值仍回落 `Paused`。
- 对端消息格式尚无网络消费者，可直接替换为 tid 版本，不做兼容层。
- 回滚：移除新增表与网络启动路径即可回到当前本地状态机；旧传输记录仍可读取。

## Open Questions

- 取消后是否删除已完成文件（当前决定：保留，只删临时文件）。
- 是否在 v1 保留空目录与文件权限（当前决定：不保留，文档标注）。
- 真机验收前是否先做最小 `.app` 打包（当前决定：验收阶段必须做）。
