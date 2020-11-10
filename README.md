## Log Manager

Log Manager is a Greengrass component that manages the system as well as user component logs. 
Lambda Manager is an optional internal Greengrass service that runs in the same JVM as the 
[Greengrass nucleus](https://github.com/aws/aws-greengrass-nucleus).

Log Manager has two major features: **Logs Uploader** and **Disk Space Management**
  
**Logs Uploader** --
It is responsible for uploading logs from the device from greengrass as well as non-greengrass components to CloudWatch.
Since the customers can use either the Greengrass Logging and Metrics service framework or any other framework to log, the 
logs uploader needs to be smart in order to handle these different formats of logs. 
The logs uploader should be able to handle any network disruptions or device reboots. The logs uploader should smartly
manage the log rotation for different logging frameworks and upload the logs on a “best effort” basis.
 
The customers can add each components configuration for where the log files are location and how they are rotated. The
logs uploader will then perform a k-way merge and update the logs to CloudWatch in batches. After merging the different 
log files the logs uploader will create the log groups and log streams as needed before pushing all the log events to
CloudWatch.

**Disk Space Management** --
This feature is responsible for managing the space taken by the logs on the device. The customers can configure the log manager
to delete the log files after all the logs from it have been uploaded to CloudWatch. The customers can also configure
the log manager to manage the disk space taken by the log files on the disk. The log manager will try to keep the logs below
the threshold specified by the customer.

## FAQ

## Sample Configuration
```
Manifests:
  - Dependencies:
      aws.greengrass.logmanager
  - aws.greengrass.logmanager:
      Parameters:
        logsUploaderConfiguration: 
          componentLogInformation:
            componentName: 'ComponentName'
            logFileRegex: 'fileNameRegex'
            logFileDirectoryPath: '/path/to/file'
            minimumLogLevel: 'INFO'
            diskSpaceLimit: '25'
            diskSpaceLimitUnit: 'MB'
            deleteLogFileAfterCloudUpload: true
          systemLogsConfiguration:
            uploadToCloudWatch: true
            minimumLogLevel: 'INFO'
            diskSpaceLimit: '25'
            diskSpaceLimitUnit: 'MB'
            deleteLogFileAfterCloudUpload: true
        
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

