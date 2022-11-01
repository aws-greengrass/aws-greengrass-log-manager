User Acceptance Tests (UATs)
UATs are defined under uat module. 
UATs use aws-greengrass-testing-standalone as the test framework to run the tests.
aws-greengrass-testing-standalone is pulled as maven dependency from GG maven repo. 
You can add/update UATs at uat source. 
The uat module generates a UAT artifact (nucleus-uat-artifact.jar) which is an executable jar meant to run the UATs.

Running UATs locally
UAT runs require the credentials for the AWS account you want to use. 
Ensure credentials are available in the environment. 
Command to run UATs locally from the project root is:

java -Dggc.archive=/home/ec2-user/aws-greengrass-nucleus.zip
-Dtest.log.path=/home/ec2-user -jar /home/ec2-user/greengrass-log-manager-uat-artifact.jar 
-Dtags=NucleusSnapshotUat -Dggc.install.root=$CODEBUILD_SRC_DIR 
-Dggc.log.level=INFO -Daws.region=us-east-1 
-jar uat/target/nucleus-uat-artifact.jar

Explanation for the above path as below:

-Dggc.archive=/home/ec2-user/aws-greengrass-nucleus.zip→ path to the Nuclues Zip
-Dtest.log.path=/home/ec2-user→ path where the test results are being stored
-jar /home/ec2-user/greengrass-log-manager-uat-artifact.jar→ log manager jar
