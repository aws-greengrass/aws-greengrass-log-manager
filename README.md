## AWS Greengrass Log Manager

A Generic Type component for AWS IoT Greengrass that tails log files and EMF JSON files, uploading them to Amazon CloudWatch Logs.

The log manager component collects and uploads logs from Greengrass core devices to Amazon CloudWatch Logs.
It tails configured log directories, batches events respecting CloudWatch API limits, and manages disk space
by deleting uploaded files when configured.

### Features

**Logs Uploader** — Scans configured directories for log files matching a regex pattern, reads new content
from persisted byte offsets, batches events (1MB / 10K events / 23h span limits), and uploads via PutLogEvents.
Supports multi-line log assembly, EMF JSON passthrough, and minimumLogLevel filtering for JSON-structured
Greengrass logs.

**Checkpoint Persistence** — Tracks per-file read offsets using content-hash-based file identity (survives
log rotation). Atomic checkpoint writes with fsync for crash safety. Supports both current and deprecated
checkpoint formats (controlled by `deprecatedVersionSupport` configuration).

**Disk Space Management** — Enforces per-source disk limits by deleting oldest fully-uploaded files
when the configured `diskSpaceLimit` is exceeded after a successful upload cycle.
Supports `deleteLogFileAfterCloudUpload` for automatic cleanup of successfully uploaded files.

### Sample Configuration

```json
{
  "logsUploaderConfiguration": {
    "systemLogsConfiguration": {
      "uploadToCloudWatch": "true",
      "logFileDirectoryPath": "/greengrass/v2/logs",
      "logFileRegex": "greengrass\\.log",
      "minimumLogLevel": "INFO",
      "diskSpaceLimit": "25",
      "diskSpaceLimitUnit": "MB"
    },
    "componentLogsConfigurationMap": {
      "aws.greengrass.SystemHealth": {
        "logFileDirectoryPath": "/var/log/gg-metrics/system-health/",
        "logFileRegex": ".*\\.emf\\.json",
        "minimumLogLevel": "DEBUG",
        "diskSpaceLimit": "100",
        "diskSpaceLimitUnit": "MB",
        "deleteLogFileAfterCloudUpload": "true"
      }
    }
  },
  "periodicUploadIntervalSec": "300"
}
```

### GG Lite Note

On GG Lite devices, TES requires an explicit port configuration in `/etc/greengrass/config.yaml`:

```yaml
services:
  aws.greengrass.TokenExchangeService:
    configuration:
      port: 8090
```

### Building

Requires [Rust](https://rustup.rs/) and [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) for cross-compilation.

```bash
# ARM64 Linux (aarch64)
cargo zigbuild --release --target aarch64-unknown-linux-musl

# x86_64 Linux
cargo zigbuild --release --target x86_64-unknown-linux-musl
```

Cross-compilation produces statically linked binaries for deployment on Greengrass devices.

### Testing

```bash
# Run tests (requires Linux target or Linux host)
cargo test --target aarch64-unknown-linux-musl
```

### End-to-End Testing

For testing on a real Greengrass device (Classic or Lite) in a container, follow the setup guides in [greengrass-agent-context-pack](https://github.com/aws-greengrass/greengrass-agent-context-pack).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
