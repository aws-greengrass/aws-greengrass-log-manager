@LogManager
Feature: Greengrass V2 LogManager

    As a customer, I can configure the log manager to upload my component logs to cloudwatch

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass

    Scenario: LogManager-1-T1: component logs are uploaded to cloudwatch
        Given I create a log directory for component UserComponentM
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
                            "logFileRegex": "UserComponentM\\w*.log",
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
        And the Greengrass deployment is COMPLETED on the device after 2 minutes
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
        And the local Greengrass deployment is SUCCEEDED on the device after 60 seconds
        Then I verify that it created a log group of type GreengrassSystemComponent for component System, with streams within 60 seconds in CloudWatch
        And I verify that it created a log group of type UserComponent for component UserComponentM, with streams within 60 seconds in CloudWatch
        And I verify 20 logs for UserComponentM of type UserComponent have been uploaded to Cloudwatch within 60 seconds

    Scenario: LogManager-1-T2: Files are deleted after successfully being uploaded to cloudwatch
        Given I create a log directory for component UserComponentX
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
                            "logFileRegex": "UserComponentX\\w+.log",
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
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentX",
                "WriteFrequencyMs": "1000",
                "LogsDirectory": "${UserComponentXLogDirectory}",
                "NumberOfLogLines": "20"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 60 seconds
        And I verify the rotated files are deleted and that the active log file is present for component UserComponentX
        And I verify 20 logs for UserComponentX of type UserComponent have been uploaded to Cloudwatch within 80 seconds

    Scenario: LogManager-1-T3: As a customer I can configure the logs uploader to delete log oldest log files to keep the
        disk space limit configured by the customer
        Given 10 temporary rotated log files for component UserComponentB have been created
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
                            "diskSpaceLimit":"100",
                            "diskSpaceLimitUnit":"KB"
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
                "periodicUploadIntervalSec": "500"
            }
        }
        """
        And I deploy the Greengrass deployment configuration
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I wait 5 seconds
        Then I verify that 10 log files for component UserComponentB are still available

    @network
    Scenario: LogManager-1-T4: As a developer, logs uploader will handle network interruptions gracefully and upload logs from the last uploaded log after network resumes
        Given I create a log directory for component UserComponentX
        And I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.LogManager |  classpath:/greengrass/recipes/recipe.yaml |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
                     "componentLogsConfigurationMap": {
                        "UserComponentY": {
                            "logFileRegex": "UserComponentY\\w+.log",
                            "logFileDirectoryPath": "${UserComponentBLogDirectory}",
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
        Then the Greengrass deployment is COMPLETED on the device after 2 minutes
        Then I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        When device network connectivity is offline
        And I install the component LogGenerator from local store with configuration
        """
        {
           "MERGE":{
                "LogFileName": "UserComponentY",
                "WriteFrequencyMs": "1000",
                "LogsDirectory": "${UserComponentYLogDirectory}",
                "NumberOfLogLines": "20"
           }
        }
        """
        And the local Greengrass deployment is SUCCEEDED on the device after 60 seconds
        When device network connectivity is online
        Then I verify that it created a log group of type GreengrassSystemComponent for component System, with streams within 60 seconds in CloudWatch
        Then I verify that it created a log group of type UserComponent for component UserComponentY, with streams within 60 seconds in CloudWatch
        And I verify 20 logs for UserComponentY of type UserComponent have been uploaded to Cloudwatch within 80 seconds

