package by.avorakh.aws.kinesis.svc;

import org.jetbrains.annotations.NotNull;

public interface KinesisManageTestService {

    void deleteAllStreams() throws InterruptedException;

    void prepareStream(@NotNull String streamName, int shardCount) throws InterruptedException;
}
