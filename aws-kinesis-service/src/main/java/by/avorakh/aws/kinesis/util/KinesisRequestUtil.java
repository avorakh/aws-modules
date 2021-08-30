package by.avorakh.aws.kinesis.util;

import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.*;

import java.security.InvalidParameterException;
import java.util.Base64;

@UtilityClass
public class KinesisRequestUtil {

    private final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

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
        @NotNull String shardIterator,
        int limit
    ) {

        return GetRecordsRequest.builder()
            .shardIterator(shardIterator)
            .limit(limit)
            .build();
    }

    public @NotNull ListShardsRequest toListShardsRequest(@NotNull String streamName) {

        return ListShardsRequest.builder()
            .streamName(streamName)
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

    public @NotNull GetRecordsRequest toGetRecordsRequest(@NotNull String shardIterator) {

        return GetRecordsRequest.builder()
            .shardIterator(shardIterator)
            .build();
    }

    public byte @NotNull [] encodeBase64(@NotNull String data) {

        return BASE64_ENCODER.encode(data.getBytes());
    }

    public byte @NotNull [] decodeBase64(@NotNull String data) {

        return BASE64_DECODER.decode(data.getBytes());
    }
}
