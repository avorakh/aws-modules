package by.avorakh.aws.kinesis.svc.impl;

import by.avorakh.aws.kinesis.KinesisRecord;
import by.avorakh.aws.kinesis.svc.KinesisConsumer;
import by.avorakh.aws.kinesis.util.KinesisRequestUtil;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SimpleSimpleKinesisConsumer extends SimpleKinesisStreamInformater implements KinesisConsumer {

    @NotNull String streamName;

    @Override
    public @NotNull List<KinesisRecord> getRecords(
        @NotNull String shardId
    ) {

        validateStream(streamName);

        var listShards = getShardList();

        if (!listShards.isEmpty() && listShards.contains(shardId)) {

            var shardIterator = getShardIterator(shardId);
            log.warn("Shard: '{}', Iterator: '{}'", shardId, shardIterator);

            var recoredsResponse = kinesisClient.getRecords(KinesisRequestUtil.toGetRecordsRequest(shardIterator));

            log.warn("Shard: '{}', records response: '{}'.", shardId, recoredsResponse);

            return recoredsResponse.records()
                .stream().map(record -> toKinesisRecord(shardId, record))
                .collect(Collectors.toList());
        }

        throw new RuntimeException("Empty shard");
    }

    @Override
    public @NotNull KinesisRecord getRecord(
        @NotNull String shardId,
        @NotNull String sequenceNumber
    ) {

        var records = getRecords(shardId);

        return records.stream()
            .filter(record -> record.getSequenceNumber().equals(sequenceNumber))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Record is not found by sequenceNumber"));
    }

    public @NotNull List<String> getShardList() {

        var listShardsResponse = kinesisClient.listShards(KinesisRequestUtil.toListShardsRequest(streamName));

        return listShardsResponse.shards()
            .stream()
            .map(Shard::shardId)
            .collect(Collectors.toList());
    }

    public @NotNull String getShardIterator(@NotNull String shardId) {

        var shardIteratorResponse = kinesisClient.getShardIterator(
            KinesisRequestUtil.toGetShardIteratorRequest(streamName, shardId)
        );

        return shardIteratorResponse.shardIterator();
    }

    private @NotNull KinesisRecord toKinesisRecord(@NotNull String shardId, @NotNull Record record) {

        return new KinesisRecord(shardId, record.sequenceNumber(), record.partitionKey(), record.data().asByteArray());
    }

    public SimpleSimpleKinesisConsumer(
        @NotNull KinesisClient kinesisClient,
        @NotNull String streamName
    ) {

        super(kinesisClient);
        this.streamName = streamName;

        validateStream(streamName);
    }
}
