@LogManager
Feature: Greengrass V2 LogManager

    As a customer, I want to selectively upload my logs and metrics to AWS Cloudwatch to save cost.

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass
        And 5 temporary rotated log files for component aws.greengrass.Nucleus have been created
        And 5 temporary rotated log files for component UserComponentA have been created

    Scenario: LogManager-1-T1: configure the log manager component using a componentLogsConfiguration list and logs are uploaded to
    CloudWatch
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.LogManager | LATEST |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                     "componentLogsConfigurationMap": {
                        "UserComponentA": {
                            "logFileRegex": "UserComponentA_\\w*.log",
                            "logFileDirectoryPath": "${UserComponentALogDirectory}",
                            "deleteLogFileAfterCloudUpload": "false"
                        }
                    },
                    "systemLogsConfiguration": {
                        "uploadToCloudWatch": "true",
                        "minimumLogLevel": "INFO",
                        "diskSpaceLimit": "25",
                        "diskSpaceLimitUnit": "MB",
                        "deleteLogFileAfterCloudUpload": "true"
                        }
                },
                "periodicUploadIntervalSec": "10"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        Then I verify that it created a log group for component type GreengrassSystemComponent for component System, with streams within 120 seconds in CloudWatch
        And I verify that it created a log group for component type UserComponent for component UserComponentA, with streams within 120 seconds in CloudWatch

    Scenario: LogManager-1-T1-b: As a customer I can configure the logs uploader component using a componentLogsConfigurationMap and logs are uploaded to CloudWatch
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.LogManager     | LATEST |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
            """
            {
                "MERGE": {
                    "logsUploaderConfiguration": {
                        "componentLogsConfigurationMap": {
                             "UserComponentA": {
                            "logFileRegex": "UserComponentA_\\w*.log",
                            "logFileDirectoryPath": "${UserComponentALogDirectory}"
                             }
                        },
                            "systemLogsConfiguration": {
                                "uploadToCloudWatch":"true",
                                "minimumLogLevel":"INFO",
                                "diskSpaceLimit":"25",
                                "diskSpaceLimitUnit":"MB",
                                "deleteLogFileAfterCloudUpload":"true"
                            }
                    },
                    "periodicUploadIntervalSec": "10"
                }
            }
            """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 3 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        Then I verify that it created a log group for component type GreengrassSystemComponent for component System, with streams within 120 seconds in CloudWatch
        And I verify that it created a log group for component type UserComponent for component UserComponentA, with streams within 120 seconds in CloudWatch

    @smoke
    Scenario: LogManager-1-T2: As a customer I can configure the logs uploader to delete log files after all logs from the file have been uploaded to CloudWatch
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.LogManager | LATEST |
        And I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
            """
            {
                "MERGE": {
                    "logsUploaderConfiguration": {
                        "componentLogsConfigurationMap": {
                            "UserComponentA": {
                                "logFileRegex": "UserComponentA_\\w*.log",
                                "logFileDirectoryPath":"${UserComponentALogDirectory}",
                                "deleteLogFileAfterCloudUpload":"true"
                            }
                        },
                        "systemLogsConfiguration": {
                            "uploadToCloudWatch":"true",
                            "minimumLogLevel":"INFO",
                            "diskSpaceLimit":"25",
                            "diskSpaceLimitUnit":"MB",
                            "deleteLogFileAfterCloudUpload":"true"
                        }
                    },
                    "periodicUploadIntervalSec": "10"
                }
            }
            """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 5 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I verify that it created a log group for component type UserComponent for component UserComponentA, with streams within 120 seconds in CloudWatch
        And I verify the rotated files are deleted and that the active log file is present for component UserComponentA

