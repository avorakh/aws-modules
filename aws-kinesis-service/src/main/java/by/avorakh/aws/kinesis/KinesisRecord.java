package by.avorakh.aws.kinesis;

import lombok.Value;
import org.jetbrains.annotations.NotNull;

@Value
public class KinesisRecord {

    @NotNull String shardId;
    @NotNull String sequenceNumber;
    @NotNull String partitionKey;
    byte @NotNull [] bytes;
}
