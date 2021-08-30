package by.avorakh.aws.kinesis.svc;

import by.avorakh.aws.kinesis.KinesisStreamStatus;
import org.jetbrains.annotations.NotNull;

public interface KinesisManageService {

    @NotNull KinesisStreamStatus createStream(@NotNull String streamName, int shardCount);

    @NotNull KinesisStreamStatus deleteStream(@NotNull String streamName);
}
