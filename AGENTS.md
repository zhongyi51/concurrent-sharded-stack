# AGENTS.md

## Cursor Cloud specific instructions

### 产品概览

本仓库是单个 Rust 库 crate：`concurrent-sharded-stack`（无锁分片 Treiber 栈）。没有 Web 服务、数据库或 Docker Compose；端到端验证即 **Cargo 构建 + 测试**。

### 必需工具

- **Rust stable ≥ 1.85**（`Cargo.toml` 中 `edition = "2024"`、`rust-version = "1.85"`）
- 组件：`rustfmt`、`clippy`（与 CI 一致）

VM 若默认工具链过旧（例如 1.83），需执行：

```sh
rustup toolchain install stable --profile minimal -c rustfmt -c clippy
rustup default stable
```

### 常用命令（与 `.github/workflows/ci.yml` 对齐）

| 目的 | 命令 |
|------|------|
| 构建 | `RUSTFLAGS="-D warnings" cargo build --all-targets` |
| 测试 | `RUSTFLAGS="-D warnings" cargo test --all-targets` |
| 文档测试 | `cargo test --doc` |
| 格式化检查 | `cargo fmt --all -- --check` |
| Lint | `RUSTFLAGS="-D warnings" cargo clippy --all-targets -- -D warnings` |
| 文档构建 | `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features` |
| 基准测试（可选） | `cargo bench` |
| 性能示例（可选） | `cargo run --example profile_mpmc -- --threads 4 --duration 2` |
| Miri（可选，需 nightly） | `MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-permissive-provenance -Zmiri-disable-isolation -Zmiri-ignore-leaks" cargo miri test` |

### 服务

**无需启动任何外部服务。** 所有目标均在单进程内通过 Cargo 完成。

### 注意事项

- `cargo test --all-targets` 会运行 Criterion 基准 harness（较慢但属正常行为）。
- `examples/profile_mpmc` 为对象池负载二进制，供外部采样分析器使用；短跑 `--duration 2` 即可验证环境。
- Miri 需单独安装 nightly + miri 组件，仅 CI 的 `miri` job 需要，本地开发非必需。
