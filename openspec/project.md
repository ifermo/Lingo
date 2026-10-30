# Project Context

## Purpose

Lingo 是一个 macOS 设备间文件传输应用：同一局域网优先直连，跨网络时通过自建会合/中继服务建立加密通道；支持暂存、附言、暂停/恢复、断点续传、设备配对与后台常驻。

## Tech Stack

- Rust (edition 2024, MSRV 1.92)
- Slint 1.18（原生 UI、系统托盘、拖放、无障碍）
- tokio（核心运行时）、flume（UI/核心桥）
- rusqlite/SQLite（本地状态）、ed25519-dalek（设备身份）
- cargo-nextest, cargo-deny, typos, git-cliff
- pre-commit, GitHub Actions (macOS CI)

## Project Conventions

### Code Style

- 使用 `rustfmt` 格式化；`clippy -D warnings` 作为硬门禁。
- UI 文案、日志与注释使用英文；面向用户的说明文档可双语（README.md 英文，README.zh.md 中文）。
- 领域类型不使用 `unwrap`/`expect` 处理外部输入；错误通过 `thiserror` 建模。

### Architecture Patterns

- `lingo-core` 不依赖 UI；UI 依赖 core 的只读快照与命令枚举。
- Slint 只在主线程更新；后台任务通过 `slint::invoke_from_event_loop` 投递。
- 传输进度按固定频率采样并合并，禁止逐字节刷新 UI。
- 所有文件写入使用临时文件 + 原子重命名；manifest 路径必须经过规范化与越界校验。

### Testing Strategy

- 使用 `cargo nextest`；core 单元测试覆盖路径安全、manifest、消息编解码、身份与存储。
- UI 采用组件画廊 + `slint-viewer --screenshot` 做像素基线；性能数据只认 release 构建。

### Git Workflow

- 使用小步提交与 Conventional Commits；提交前跑 fmt/clippy/nextest。

## Domain Context

- **设备（Device）**：Ed25519 身份，DID = base64url(公钥)；同一账号可有多台设备。
- **暂存（Staging）**：待发送文件集合，包含 manifest 与源路径映射。
- **传输（Transfer）**：role 为 src/dst，状态为 paused/ready/canceled/complete；双端各自维护状态。
- **链接码（Link code）**：通过服务端解析为 DID 的短期配对码。
- **会合（Rendezvous）**：交换候选地址、打洞、失败回退中继。

## Important Constraints

- 不复用 Payload 的私有协议、服务端、品牌或视觉资产；仅做功能等价实现。
- Slint 许可为 GPL-3.0 / Royalty-free / Software 三选一，闭源发布前必须完成许可决策。
- 中继带宽与推送是持续成本，需要配额、限速与滥用检测。
- macOS 局域网发现需要 `NSLocalNetworkUsageDescription` 与 `NSBonjourServices`。

## External Dependencies

- crates.io
- GitHub Actions marketplace actions
- RustSec advisory database (cargo-deny)
- 自建 API/中继服务（后续里程碑）
