## Log Manager User Acceptance Tests
User Acceptance Tests for Log Manager run using `aws-greengrass-testing-standalone` as a library. They execute E2E
tests which will spin up an instance of Greengrass on your device and execute different sets of tests, by installing
the `aws.greengrass.LogManager` component.

## Running UATs locally

Ensure credentials are available by setting them in environment variables. In unix based systems:

```bash
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```

on Windows Powershell

```bash
$Env:AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
$Env:AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```

For UATs to run you will need to package your entire application along with `aws-greengrass-testing-standalone` into
an uber jar. To do run (from the root of the project)

```
mvn -U -ntp clean verify -f uat/pom.xml
```

Note: Everytime you make changes to the codebase you will have to rebuild the uber jar for those changes to be present
on the final artifact.

Finally, download the zip containing the latest version of the Nucleus, which will be used to provision Greengrass for
the UATs.

```bash
curl -s https://d2s8p88vqu9w66.cloudfront.net/releases/greengrass-nucleus-latest.zip > greengrass-nucleus-latest.zip
```

Execute the UATs by running the following command from the root of the project.

```
sudo java -Dggc.archive=<path-to-nucleus-zip> -Dtest.log.path=<path-to-test-results-folder> -Dtags=LogManager -jar uat/features/target/log-manager-uats.jar
```

Command arguments:

Dggc.archive - path to the nucleus zip that was downloaded
Dtest.log.path - path where you would like the test results to be stored 