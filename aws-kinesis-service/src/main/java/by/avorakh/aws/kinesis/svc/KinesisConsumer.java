package by.avorakh.aws.kinesis.svc;

import by.avorakh.aws.kinesis.KinesisRecord;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface KinesisConsumer {

    @NotNull List<KinesisRecord> getRecords(@NotNull String shardId);

    @NotNull KinesisRecord getRecord(@NotNull String shardId, @NotNull String sequenceNumber);
}
