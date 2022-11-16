package com.aws.greengrass.testing.resources.cloudwatch;

import com.aws.greengrass.testing.api.model.TestingModel;
import com.aws.greengrass.testing.resources.AWSResources;
import com.aws.greengrass.testing.resources.ResourceSpec;
import org.immutables.value.Value;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import javax.annotation.Nullable;

@TestingModel
@Value.Immutable
public interface CloudWatchLogStreamSpecModel extends ResourceSpec<CloudWatchLogsClient, CloudWatchLogStream> {

    @Nullable
    @Override
    CloudWatchLogStream resource();

    @Override
    default CloudWatchLogStreamSpec create(CloudWatchLogsClient client, AWSResources resources) {
        // Not implementing yet given we don't create the streams ourselves on tests. They get created by
        // the components
        return CloudWatchLogStreamSpec.builder().build();
    }
}