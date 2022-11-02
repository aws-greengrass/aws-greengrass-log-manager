##User Acceptance Test
UATs are defined under uat module. 
UATs use aws-greengrass-testing-standalone as the test framework to run the tests.
aws-greengrass-testing-standalone is pulled as maven dependency from GG maven repo. 
You can add/update UATs at uat source. 
The uat module generates a UAT artifact (nucleus-uat-artifact.jar) which is an executable jar meant to run the UATs.

##Running UATs locally
* UAT runs require the credentials for the AWS account you want to use.
* Ensure credentials are available in the environment.

For UATs to run you will need to package your entire application along with OTF as a dependency on an Uber jar. 
To do that run following commands from the root of the project:

```
mvn -U -ntp clean verify -f uat/pom.xml
mvn -U -ntp verify -DskipTests=true
```

Note: Everytime you make changes to the code base you will have to rebuild the Uber jar for those changes to be present.

Command to run UATs locally from the project root is:

```
java -Dggc.archive=<path to nuclues zip> -Dtest.log.path=<path to the test results> -jar uat/target/greengrass-log-manager-uat-artifact.jar
```

Command arguments:

Dggc.archive - path to the nucleus zip
Dtest.log.path - path where the test results are being stored
Dtags - filter tests by a specific cucumber tag (HelloWorldTst in above command)