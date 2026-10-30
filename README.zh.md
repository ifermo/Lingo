# Lingo

面向 macOS 的设备间文件传输应用，使用 Rust + Slint 实现。

[English](README.md)

## 当前状态

M0/M1 已完成：Slint 界面、core 领域层、加密传输原语与集成测试。下一步把 mDNS 发现与文件传输接入应用，再接会合/中继服务。

## 架构

- `crates/lingo-core` — 领域模型、消息信封、SQLite 存储、设备身份、manifest 与路径安全。
- `crates/lingo-ui` — Slint 组件与 ViewModel。
- `crates/lingo-net` — QUIC/TLS 端点、自签证书、应用层 Ed25519 身份认证、mDNS 发现、帧协议。
- `crates/lingo-app` — 桌面二进制、tokio 运行时、平台集成。

UI 运行在 Slint 事件循环，核心逻辑运行在 tokio 运行时，通过有界 channel 桥接；隐藏窗口 + 系统托盘用于后台常驻。

## 快速开始

```bash
cargo run -p lingo-app
```

画廊模式（假数据，便于截图核对）：

```bash
cargo run -p lingo-app -- --gallery
```

## 质量门禁

```bash
cargo fmt -- --check
cargo check --all-targets
cargo clippy --all-targets --all-features --tests --benches -- -D warnings
cargo nextest run --all-features
cargo deny check bans licenses sources
typos
```

## 开发工具

```bash
cargo install --locked cargo-deny
cargo install cargo-nextest --locked
cargo install typos-cli
cargo install git-cliff
pipx install pre-commit && pre-commit install
```

## CI

GitHub Actions 在 push / pull request 时于 macOS 运行同一套门禁。

## 许可

源码采用 MIT OR Apache-2.0。第三方许可以及 Slint 许可决策记录在 [docs/licensing.md](docs/licensing.md)。
