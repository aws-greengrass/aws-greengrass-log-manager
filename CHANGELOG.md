# Changelog

## v2.1.0

### Bug fixes and improvements

* Use defaults for logFileDirectoryPath and logFileRegex that work for Greengrass components that print to standard output (stdout) and standard error (stderr). 
* Correctly route traffic through a configured network proxy when uploading logs to CloudWatch Logs. 
* Correctly handle colon characters (:) in log stream names. CloudWatch Logs log stream names don't support colons. 
* Simplify log stream names by removing thing group names from the log stream. 
* Remove an error log message that prints during normal behavior.
