# Change: 打通局域网端到端文件传输

## Why

M0/M1 已交付 Slint 外壳、领域/持久化层与传输原语，但产品主链路尚未闭环：`lingo-net` 没有被桌面应用依赖，`Service` 只写数据库，用户无法真正发送或接收文件。文件传输是当前唯一的核心价值缺口。

## What Changes

- 将 `lingo-net` 接入桌面应用，绑定局域网地址并启动 mDNS 发现。
- 将可重放的自选 nonce `AuthProof` 改为挑战-应答，并绑定 QUIC TLS exporter。
- 新增认证会话与传输控制协议，所有对端消息以 transfer UUID 关联。
- 新增逐文件双向流数据面：分块传输、SHA-256 校验、临时文件 + 原子重命名、路径安全与重名避让。
- 实现接收方确认/拒绝、暂停/恢复/取消、进度/速度、失败原因。
- 以接收方偏移为准持久化断点，应用重启后可恢复。
- UI 接入真实设备、入站确认、进度与错误。

不在本期范围：链接码配对、会合/中继服务、配额/推送、系统通知、后台开关持久化。

## Impact

- 代码：`crates/lingo-net`、`crates/lingo-core`、`crates/lingo-app`、`crates/lingo-ui`
- 数据：新增 `transfer_files` 表；`TransferStatus` 增加 `pending` / `failed`
- 平台：macOS 本地网络权限与 `NSBonjourServices`（真机验收所需）
- 规格：新增 `peer-discovery`、`peer-session`、`file-transfer`、`transfer-recovery`
