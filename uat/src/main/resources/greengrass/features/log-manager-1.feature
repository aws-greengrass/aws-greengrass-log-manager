@LogManager
Feature: Greengrass V2 LogManager

    As a customer, I want to selectively upload my logs and metrics to AWS Cloudwatch to save cost.

    Background:
        Given my device is registered as a Thing
        And my device is running Greengrass
        And 5 temporary rotated log files for component aws.greengrass.Nucleus have been created

    Scenario: configure the log manager component using a componentLogsConfiguration list and logs are uploaded to
    CloudWatch
        Given I create a Greengrass deployment with components
            | aws.greengrass.Cli        | LATEST |
            | aws.greengrass.LogManager | LATEST |
        When I update my Greengrass deployment configuration, setting the component aws.greengrass.LogManager configuration to:
        """
        {
            "MERGE": {
                "logsUploaderConfiguration": {
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
        And I verify the aws.greengrass.LogManager component is RUNNING using the greengrass-cli
        And I verify that it created a log group for component type GreengrassSystemComponent for component System, with streams within 120 seconds in CloudWatch