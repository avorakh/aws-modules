package by.avorakh.aws.kinesis.svc;

import by.avorakh.aws.kinesis.KinesisStreamStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.util.List;

public interface KinesisManageService {

    @NotNull KinesisStreamStatus createStream(@NotNull String streamName, int shardCount);




    @NotNull List<String> getStreamNames();

    @NotNull KinesisStreamStatus deleteStream(@NotNull String streamName);

    @Nullable StreamDescription getStreamInfo(@NotNull String streamName);


}
