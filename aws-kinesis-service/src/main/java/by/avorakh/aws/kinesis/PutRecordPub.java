package by.avorakh.aws.kinesis;

import lombok.Builder;
import lombok.Value;
import org.jetbrains.annotations.Nullable;

@Value
@Builder
public class PutRecordPub {

    boolean putted;
    @Nullable String shardId;
    @Nullable String sequenceNumber;
}
