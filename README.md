# AWS Greengrass Log Manager (Rust)

A Generic Type component for AWS IoT Greengrass that tails log files and EMF JSON files, uploading them to Amazon CloudWatch Logs.

This is a Rust reimplementation of `aws.greengrass.LogManager`, targeting minimal binary size and memory footprint for resource-constrained devices running GG Classic or GG Lite.

## Building

Requires [Rust](https://rustup.rs/) and [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) for cross-compilation.

```bash
# ARM64 Linux (aarch64)
cargo zigbuild --release --target aarch64-unknown-linux-musl

# x86_64 Linux
cargo zigbuild --release --target x86_64-unknown-linux-musl
```

Cross-compilation produces statically linked binaries for deployment on Greengrass devices.

## Testing

```bash
# Run tests (requires Linux target or Linux host)
cargo test --target aarch64-unknown-linux-musl
```

## Module Layout

| Module | Description |
|--------|-------------|
| `config/` | Configuration schema, parsing, and validation |
| `scanner/` | Log file discovery, reading, multiline assembly, checkpointing |
| `uploader/` | CloudWatch Logs batching, upload, retry |
| `credentials/` | AWS credential retrieval via GG TES |
| `disk/` | Disk space management and log cleanup |

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
