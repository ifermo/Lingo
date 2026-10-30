# Lingo

Device-to-device file transfer for macOS, built with Rust and Slint.

[中文说明](README.zh.md)

## Status

M0/M1 complete: Slint shell, core domain, encrypted transport primitives and
integration tests. Next is wiring mDNS discovery and file transfer into the app,
then the rendezvous/relay service.

## Architecture

- `crates/lingo-core` — domain types, message envelope, SQLite store, device identity, manifest/path safety.
- `crates/lingo-ui` — Slint components and view models.
- `crates/lingo-net` — QUIC/TLS endpoint, self-signed certificates, application-level Ed25519 authentication, mDNS discovery, framing.
- `crates/lingo-app` — desktop binary, tokio runtime, platform integration.

The UI runs on the Slint event loop; core work runs on a tokio runtime and is
bridged through bounded channels. A hidden window plus system tray keeps the
process available in the background.

## Quick start

```bash
cargo run -p lingo-app
```

Gallery mode (mock data, useful for screenshots):

```bash
cargo run -p lingo-app -- --gallery
```

Page and modal screenshots without a window server:

```bash
cargo run -p lingo-app -- --gallery --page 3 --screenshot /tmp/devices.png
cargo run -p lingo-app -- --gallery --incoming --screenshot /tmp/incoming.png
```

Hidden tray-only start (background mode): `lingo --hidden`.

```bash
cargo run -p lingo-app -- --gallery
```

## Quality gates

```bash
cargo fmt -- --check
cargo check --all-targets
cargo clippy --all-targets --all-features --tests --benches -- -D warnings
cargo nextest run --all-features
cargo deny check bans licenses sources
typos
```

## Development tooling

```bash
cargo install --locked cargo-deny
cargo install cargo-nextest --locked
cargo install typos-cli
cargo install git-cliff
pipx install pre-commit && pre-commit install
```

## CI

GitHub Actions runs the same gates on macOS for pushes and pull requests.

## License

Source is MIT OR Apache-2.0. Third-party licenses, including the Slint licensing
decision, are documented in [docs/licensing.md](docs/licensing.md).
