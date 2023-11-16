@LogManager
Feature: Greengrass V2 LogManager

    As a customer, I can configure the log manager to upload my component logs to cloudwatch

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass

    Scenario: LogManager-1-T1-a: component logs are uploaded to cloudwatch using componentLogsConfigurationMap
    configuration key
        Given I create a log directory for component called UserComponentMLogDirectory
        And I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST                                    |
            | aws.greengrass.LogManager | classpath:/greengrass/recipes/recipe.yaml |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                     "componentLogsConfigurationMap": {
                        "UserComponentM": {
                            "logFileRegex": "UserComponentM(.*).log",
                            "logFileDirectoryPath": "${UserComponentMLogDirectory}",
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
                "periodicUploadIntervalSec": "5"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        And the Greengrass deployment is COMPLETED on the device after 3 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentM",
                "WriteFrequencyMs": "500",
                "LogsDirectory": "${UserComponentMLogDirectory}",
                "NumberOfLogLines": "20"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 180 seconds
        Then I verify that it created a log group of type GreengrassSystemComponent for component System, with streams within 60 seconds in CloudWatch
        And I verify that it created a log group of type UserComponent for component UserComponentM, with streams within 60 seconds in CloudWatch
        And I verify 20 logs for UserComponentM of type UserComponent have been uploaded to Cloudwatch within 60 seconds

    Scenario: LogManager-1-T1-b: component logs are uploaded to cloudwatch using componentLogsConfiguration
    configuration key
        Given I create a log directory for component called UserComponentWLogDirectory
        And I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST                                    |
            | aws.greengrass.LogManager | classpath:/greengrass/recipes/recipe.yaml |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                    "componentLogsConfiguration": [
                        {
                            "logFileRegex": "UserComponentW(.*).log",
                            "logFileDirectoryPath": "${UserComponentWLogDirectory}",
                            "componentName": "UserComponentW"
                        }
                    ],
                    "systemLogsConfiguration": {
                        "uploadToCloudWatch": "true",
                        "minimumLogLevel": "INFO",
                        "diskSpaceLimit": "25",
                        "diskSpaceLimitUnit": "MB",
                        "deleteLogFileAfterCloudUpload": "true"
                    }
                },
                "periodicUploadIntervalSec": "5"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        And the Greengrass deployment is COMPLETED on the device after 3 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentW",
                "WriteFrequencyMs": "500",
                "LogsDirectory": "${UserComponentWLogDirectory}",
                "NumberOfLogLines": "20"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 180 seconds
        Then I verify that it created a log group of type GreengrassSystemComponent for component System, with streams within 60 seconds in CloudWatch
        And I verify that it created a log group of type UserComponent for component UserComponentW, with streams within 60 seconds in CloudWatch
        And I verify 20 logs for UserComponentW of type UserComponent have been uploaded to Cloudwatch within 60 seconds

    Scenario: LogManager-1-T2: Files are deleted after successfully being uploaded to cloudwatch
        Given I create a log directory for component called UserComponentXLogDirectory
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST                                    |
            | aws.greengrass.LogManager | classpath:/greengrass/recipes/recipe.yaml |
        And I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
         """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                     "componentLogsConfigurationMap": {
                        "UserComponentX": {
                            "logFileRegex": "UserComponentX(.*).log",
                            "logFileDirectoryPath": "${UserComponentXLogDirectory}",
                            "deleteLogFileAfterCloudUpload": "true"
                        }
                    }
                },
                "periodicUploadIntervalSec": "10"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 3 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentX",
                "WriteFrequencyMs": "250",
                "FileSize": "5",
                "FileSizeUnit": "KB",
                "LogsDirectory": "${UserComponentXLogDirectory}",
                "NumberOfLogLines": "100"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 180 seconds
        Then I verify that it created a log group of type UserComponent for component UserComponentX, with streams within 60 seconds in CloudWatch
        And I verify 100 logs for UserComponentX of type UserComponent have been uploaded to Cloudwatch within 120 seconds
        And I verify the rotated files are deleted and that the active log file is present for component UserComponentX on directory UserComponentXLogDirectory

    Scenario: LogManager-1-T3: As a customer I can configure the logs uploader to delete log oldest log files to keep the
    disk space limit specified on the configuration
        Given 15 temporary rotated log files for component UserComponentB have been created
        And 15 temporary rotated log files for component UserComponentC have been created
        And 5 temporary rotated log files for component aws.greengrass.Nucleus have been created
        And 5 temporary rotated log files for component UserComponentA have been created
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST                                    |
            | aws.greengrass.LogManager | classpath:/greengrass/recipes/recipe.yaml |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                     "componentLogsConfigurationMap": {
                        "UserComponentB": {
                            "logFileRegex": "UserComponentB_(.)+.log",
                            "logFileDirectoryPath": "${UserComponentBLogDirectory}",
                            "diskSpaceLimit":"0",
                            "diskSpaceLimitUnit":"KB",
                            "minimumLogLevel": "WARN",
                            "uploadToCloudWatch": "true",
                            "deleteLogFileAfterCloudUpload": "false"
                        },
                        "UserComponentC": {
                            "logFileRegex": "UserComponentC_(.)+.log",
                            "logFileDirectoryPath": "${UserComponentCLogDirectory}",
                            "diskSpaceLimit":"100",
                            "diskSpaceLimitUnit":"KB",
                            "minimumLogLevel": "INFO",
                            "uploadToCloudWatch": "true",
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
                "periodicUploadIntervalSec": "0.1"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 3 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I wait 5 seconds
        Then I verify that 1 log files for component UserComponentB are still available
        And I verify that 9 log files for component UserComponentC are still available

    @network
    Scenario: LogManager-1-T4: As a developer, logs uploader will handle network interruptions gracefully and upload logs from the last uploaded log after network resumes
        Given I create a log directory for component called UserComponentYLogDirectory
        And I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST                                    |
            | aws.greengrass.LogManager | classpath:/greengrass/recipes/recipe.yaml |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                     "componentLogsConfigurationMap": {
                        "UserComponentY": {
                            "logFileRegex": "UserComponentY(.*).log",
                            "logFileDirectoryPath": "${UserComponentYLogDirectory}"
                        }
                    },
                    "systemLogsConfiguration": {
                        "uploadToCloudWatch": "true",
                        "deleteLogFileAfterCloudUpload": "true"
                    }
                },
                "periodicUploadIntervalSec": "10"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 3 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        When device network connectivity is offline
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentY",
                "WriteFrequencyMs": "500",
                "LogsDirectory": "${UserComponentYLogDirectory}",
                "NumberOfLogLines": "20"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 180 seconds
        When device network connectivity is online
        Then I verify that it created a log group of type GreengrassSystemComponent for component System, with streams within 60 seconds in CloudWatch
        Then I verify that it created a log group of type UserComponent for component UserComponentY, with streams within 60 seconds in CloudWatch
        And I verify 20 logs for UserComponentY of type UserComponent have been uploaded to Cloudwatch within 80 seconds

    Scenario: LogManager-1-T5: When more than 10_000 component logs are to be uploaded, then log manager uploads all
    the logs in multiple attempts where each attempt has a maximum of 10_000 log events.

        Given I create a log directory for component called UserComponentWLogDirectory
        And I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST                                    |
            | aws.greengrass.LogManager | classpath:/greengrass/recipes/recipe.yaml |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                    "componentLogsConfiguration": [
                        {
                            "logFileRegex": "UserComponentW(.*).log",
                            "logFileDirectoryPath": "${UserComponentWLogDirectory}",
                            "componentName": "UserComponentW"
                        }
                    ],
                    "systemLogsConfiguration": {
                        "uploadToCloudWatch": "true",
                        "minimumLogLevel": "INFO",
                        "diskSpaceLimit": "25",
                        "diskSpaceLimitUnit": "MB",
                        "deleteLogFileAfterCloudUpload": "true"
                    }
                },
                "periodicUploadIntervalSec": "300"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        And the Greengrass deployment is COMPLETED on the device after 3 minutes
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentW",
                "WriteFrequencyMs": "0",
                "LogsDirectory": "${UserComponentWLogDirectory}",
                "NumberOfLogLines": "10005"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 180 seconds
        Then I verify that it created a log group of type GreengrassSystemComponent for component System, with streams within 300 seconds in CloudWatch
        And I verify that it created a log group of type UserComponent for component UserComponentW, with streams within 300 seconds in CloudWatch
        And I verify 10000 logs for UserComponentW of type UserComponent have been uploaded to Cloudwatch within 120 seconds

    Scenario: LogManager-1-T6: Log manager can upload 200,000 logs quickly
        Given I create a log directory for component called UserComponentWLogDirectory
        And I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST                                    |
            | aws.greengrass.LogManager | classpath:/greengrass/recipes/recipe.yaml |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                    "componentLogsConfiguration": [
                        {
                            "logFileRegex": "UserComponentW(.*).log",
                            "logFileDirectoryPath": "${UserComponentWLogDirectory}",
                            "componentName": "UserComponentW"
                        }
                    ]
                },
                "periodicUploadIntervalSec": "0.001",
                "deprecatedVersionSupport": "false"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        And the Greengrass deployment is COMPLETED on the device after 3 minutes
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentW",
                "WriteFrequencyMs": "0",
                "LogsDirectory": "${UserComponentWLogDirectory}",
                "NumberOfLogLines": "200000"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 180 seconds
        And I verify that it created a log group of type UserComponent for component UserComponentW, with streams within 300 seconds in CloudWatch
        And I verify 200000 logs for UserComponentW of type UserComponent have been uploaded to Cloudwatch within 120 seconds
        # Can achieve ~1.4MBps running on codebuild, presumably due to high upload speed and low ping time.
        # This test may fail when run on your own computer as your ping time to CloudWatch will be higher.
        And I verify that logs for UserComponentW of type UserComponent uploaded with a rate greater than 1.0 MBps
