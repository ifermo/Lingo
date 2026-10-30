## 1. 方案冻结

- [x] 1.1 确认本期范围为局域网直连；链接码、会合/中继、配额/推送、通知明确不做
- [x] 1.2 冻结控制消息、文件流消息、错误码与状态机
- [x] 1.3 冻结 `transfer_files` schema、迁移与启动恢复规则
- [x] 1.4 冻结信任模型：发现结果不可信，认证 DID 为准，首次入站需用户确认

## 2. 会话与发现

- [x] 2.1 在 `crates/lingo-app/Cargo.toml` 增加 `lingo-net` 依赖
- [x] 2.2 `PeerEndpoint` 支持配置绑定地址，默认 `0.0.0.0:0`，暴露实际端口
- [x] 2.3 改造 `auth.rs`：双方挑战、挑战-应答签名、`export_keying_material` 绑定；删除自选 nonce 的直接验证路径
- [x] 2.4 新增 `lingo-net::session`：握手、认证后控制流、帧收发、错误与超时
- [x] 2.5 新增发现事件循环：订阅 `ServiceEvent`，解析地址、端口、TXT（owner/name/variant/color），维护上下线
- [x] 2.6 新增 peer registry：DID → Device/会话；单 DID 单连接；认证 DID 与预期 DID 一致性校验
- [x] 2.7 测试：握手成功、重放被拒、错误挑战被拒、跨连接 exporter 不匹配、发现上下线、DID 不匹配拒绝

## 3. 状态机与持久化

- [x] 3.1 `TransferStatus` 增加 `Pending` / `Failed`；更新 store 字符串、UI 文案与测试
- [x] 3.2 对端消息改为 `tid`；新增 offer/chunk/accept/decline/control/complete/failed/resume-query/resume-state
- [x] 3.3 新增 `transfer_files` 表与 CRUD：`path`、`final_path`、`size`、`offset`、`state`、`sha256`
- [x] 3.4 `Service` 增加入站 offer 创建、接受、拒绝、peer 状态合并、完成/失败、进度更新
- [x] 3.5 修正 `effective_status` 优先级：`Failed` > `Canceled` > 双方 `Complete` > `Ready` > `Paused`，补对端取消测试
- [x] 3.6 启动恢复：`Ready` → `Paused`，源缺失/变更 → `Failed`，offset 从 `transfer_files` 恢复
- [x] 3.7 测试：状态迁移、peer 取消不被本地 Ready 掩盖、重启后 offset/状态恢复

## 4. 数据面

- [x] 4.1 源路径解析：由 manifest 路径 + `path_map` 得到本地源路径，校验普通文件与越界
- [x] 4.2 接收目标：canonicalize 目标根；接受时确定并持久化每个文件的唯一 `final_path`
- [x] 4.3 临时文件：目标根下 `.lingo-part/<tid>/`；写入、`sync_data`、原子重命名；失败保留、取消清理
- [x] 4.4 发送：打开文件、校验 size/mtime、分块读取、滚动 SHA-256、有界缓冲、进度采样
- [x] 4.5 接收：校验 `FileStart`、按对方确认的 offset 续写、滚动 SHA-256、最终校验、重命名
- [x] 4.6 并发与背压：单传输最多 4 个文件流；控制流独立；进度 200 ms 合并
- [x] 4.7 暂停/恢复/取消：重置数据流、落库 offset、暂停保留临时文件、取消删除临时目录
- [x] 4.8 错误映射与对端通知：源变更、源缺失、磁盘错误、哈希不一致、路径非法、超时、断线
- [x] 4.9 测试：单文件、目录树、空文件、Unicode 文件名、大文件、重名避让、路径逃逸、暂停恢复、取消、哈希失败
- [x] 4.10 启动时清理已完成/已取消/已移除传输的临时目录
- [x] 4.11 启动对账：最终文件存在则标记文件完成并清理临时文件；临时文件长度作为续传 offset；已声明完成但最终文件缺失则标记失败
- [x] 4.12 数据流读超时：控制帧/数据帧/完成确认均受 `read_timeout` 约束
- [x] 4.13 发送结束后复核源文件 size 与 mtime，变化则中止并通知对端
- [x] 4.14 测试：重启后续传 offset、最终文件清理临时文件、取消清理、完成文件缺失失败、读超时

## 5. 应用与 UI 接线

- [x] 5.1 app core loop 同时驱动 `Service` 与 `TransferAgent`，用 `tokio::select!` 处理 UI 命令和 agent 事件
- [x] 5.2 `ClientSend` 创建出站传输后启动发送；入站 offer 弹窗；接受目录回传 agent；拒绝通知对端
- [x] 5.3 UI 绑定真实设备、`Pending`/`Failed` 文案、进度、速度、错误原因
- [x] 5.4 “删除文件”入口接线；移除活跃传输时通知对端并清理临时文件
- [x] 5.5 回环 E2E：同一进程或两个进程模拟两设备，真实文件传输
- [x] 5.6 质量门禁：`cargo fmt -- --check`、`cargo check --all-targets`、clippy、nextest、typos

## 6. 平台与验收

- [ ] 6.1 最小 `.app` 打包与 `Info.plist`：`NSLocalNetworkUsageDescription`、`NSBonjourServices`（`_lingo._tcp`）
- [ ] 6.2 两台 Mac 真机验收：发现、发送、接收、暂停/恢复、断网重连、接收端重启续传
- [ ] 6.3 记录本期限制：无链接码/中继、不保留空目录与文件元数据、不覆盖同名文件、取消保留已完成文件

## 7. 验收标准

- [ ] 7.1 两台设备在同一局域网能在 5 s 内互相发现并展示在线状态
- [ ] 7.2 发送 1 GB 单文件期间可暂停、恢复，最终 SHA-256 一致且目标文件原子出现
- [ ] 7.3 接收端在传输中退出并重启，重新连接后从已接收偏移继续，最终文件一致
- [ ] 7.4 含 `../`、绝对路径、符号链接逃逸的 offer 被整体拒绝，目标根外无任何写入
- [ ] 7.5 重放一条历史认证证明无法建立会话；不同 QUIC 连接间不能复用同一证明
- [ ] 7.6 源文件在发送中被修改时传输明确失败，不产生“看似完成”的坏文件
- [ ] 7.7 所有新增路径均有单元或集成测试，CI 门禁通过
