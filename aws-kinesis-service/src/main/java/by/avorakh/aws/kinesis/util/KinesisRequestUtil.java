package by.avorakh.aws.kinesis.util;

import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.*;

import java.security.InvalidParameterException;

@UtilityClass
public class KinesisRequestUtil {

    public @NotNull CreateStreamRequest createStreamRequest(@NotNull String streamName, int shardCount) {

        // TODO add stream name validation (Blank_regex)

        if (shardCount < 1) {
            throw new InvalidParameterException("The shard count should be positive and greater than zero.");
        }

        return CreateStreamRequest.builder()
            .streamName(streamName)
            .shardCount(shardCount)
            .build();
    }

    public @NotNull DescribeStreamRequest toDescribeStreamRequest(@NotNull String streamName) {

        return DescribeStreamRequest.builder()
            .streamName(streamName)
            .build();
    }

    public @NotNull DeleteStreamRequest toDeleteStreamRequest(@NotNull String streamName) {

        return DeleteStreamRequest.builder()
            .streamName(streamName)
            .build();
    }

    public @NotNull PutRecordRequest toPutRecordRequest(
        @NotNull String streamName,
        @NotNull String partitionKey,
        byte @NotNull [] bytes
    ) {

        return PutRecordRequest.builder()
            .partitionKey(partitionKey)
            .streamName(streamName)
            .data(SdkBytes.fromByteArray(bytes))
            .build();
    }


    public @NotNull GetRecordsRequest toGetRecordsRequest(
        @NotNull String streamName,
        @NotNull String shardIterator
    ) {

        return GetRecordsRequest.builder()
            .shardIterator()
            .build();
    }



    public @NotNull GetShardIteratorRequest toGetShardIteratorRequest(
        @NotNull String streamName,
        @NotNull String shardId
    ) {

        return GetShardIteratorRequest.builder()
            .streamName(streamName)
            .shardId(shardId)
           .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build();
    }
}
