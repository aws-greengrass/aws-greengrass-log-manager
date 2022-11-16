package com.aws.greengrass;
import com.aws.greengrass.testing.api.model.TestingModel;
import com.aws.greengrass.testing.resources.AWSResource;
import org.immutables.value.Value;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

@TestingModel
@Value.Immutable
public interface CloudWatchLogStreamModel extends AWSResource<CloudWatchLogsClient> {
    String groupName();
    String streamName();
    @Override
    default void remove(CloudWatchLogsClient client) {
        DeleteLogStreamRequest request =
                DeleteLogStreamRequest.builder().logGroupName(groupName()).logStreamName(streamName()).build();
        try {
            client.deleteLogStream(request);
        } catch (ResourceNotFoundException notFound) {
            // Do nothing if it is already deleted.
        }
    }
}