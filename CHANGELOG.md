# Changelog

## v2.1.0

### Bug fixes and improvements:
* Use defaults for `logFileDirectoryPath` and `logFileRegex` that work for Greengrass components that print to standard output (stdout) and standard error (stderr). ([#45](https://github.com/aws-greengrass/aws-greengrass-log-manager/pull/45))([331f68b](https://github.com/aws-greengrass/aws-greengrass-log-manager/commit/331f68bd032008a56bc2541cb303f303fb483824)), closes [#41](https://github.com/aws-greengrass/aws-greengrass-log-manager/issues/41)
* Correctly route traffic through a configured network proxy when uploading logs to CloudWatch Logs. ([#47](https://github.com/aws-greengrass/aws-greengrass-log-manager/pull/47))([b0b4342](https://github.com/aws-greengrass/aws-greengrass-log-manager/commit/b0b4342b10ef87fa3d9a76481494e6a1acd7e6ff))
* Correctly form log stream names for things with multiple target thing groups. Correctly handle colon characters (`:`) in log stream names. CloudWatch Logs log stream names don't support colons. ([#49](https://github.com/aws-greengrass/aws-greengrass-log-manager/pull/49))([3be040e](https://github.com/aws-greengrass/aws-greengrass-log-manager/commit/3be040e11fc1239046f746d99766147741784749))
* Remove an error log message that prints during normal behavior. ([#48](https://github.com/aws-greengrass/aws-greengrass-log-manager/pull/48))([e52b9d1](https://github.com/aws-greengrass/aws-greengrass-log-manager/commit/e52b9d120521ebb06205a273b8349015669252e0))
