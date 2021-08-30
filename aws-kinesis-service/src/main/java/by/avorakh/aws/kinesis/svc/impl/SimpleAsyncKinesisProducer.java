package by.avorakh.aws.kinesis.svc.impl;

import by.avorakh.aws.kinesis.PutRecordPub;
import by.avorakh.aws.kinesis.svc.AsyncKinesisProducer;
import by.avorakh.aws.kinesis.util.KinesisRequestUtil;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SimpleAsyncKinesisProducer implements AsyncKinesisProducer {

    @NotNull KinesisAsyncClient kinesisClient;
    @NotNull String streamName;

    @Override
    public @NotNull CompletableFuture<PutRecordPub> putRecord(@NotNull String partitionKey, byte @NotNull [] bytes) {

        return validateStream().thenCompose(activeStream -> {

            var putRecordRequest = KinesisRequestUtil.toPutRecordRequest(
                streamName,
                partitionKey,
                bytes
            );

            if (!activeStream) {
                // TODO need to add to the queue
                log.error("Impossible to sent a record");


                return CompletableFuture.completedFuture(PutRecordPub.builder().putted(false).build());
            }

            return kinesisClient.putRecord(putRecordRequest)
                .thenApply(putRecordResponse -> {
                    log.debug(
                        "Put record - Shard Id: '{}', sequence number:'{}', encryption type: '{}'",
                        putRecordResponse.shardId(),
                        putRecordResponse.sequenceNumber(),
                        putRecordResponse.encryptionType()
                    );

                    return PutRecordPub.builder()
                        .putted(true)
                        .shardId(putRecordResponse.shardId())
                        .sequenceNumber(putRecordResponse.sequenceNumber())
                        .build();
                }); // TODO add exception handling
        });
    }

    private @NotNull CompletableFuture<Boolean> validateStream() {

        return kinesisClient.describeStream(KinesisRequestUtil.toDescribeStreamRequest(streamName))
            .thenApply(response -> {
                var streamStatus = response.streamDescription().streamStatus();

                if (!StreamStatus.ACTIVE.equals(streamStatus)) {
                    log.error(
                        "Stream '{}' is not active (Status: '{}'). Please wait a few moments and try again.",
                        streamName,
                        streamStatus
                    );
                    return Boolean.FALSE;
                }
                log.debug("Stream '{}' is active.", streamName);
                return Boolean.TRUE;
            })
            .handle((status, throwable) -> {
                if (throwable != null) {
                    var ex = throwable;

                    if (throwable instanceof CompletionException) {
                        ex = throwable.getCause();
                        log.error("Error found while validation the '{}' stream by:\n", streamName, ex);
                    }
                    log.error("Exception type: {}", ex.getClass().getTypeName());

                    throw new RuntimeException(ex);
                }

                return status;
            });
    }

    public SimpleAsyncKinesisProducer(
        @NotNull KinesisAsyncClient kinesisClient,
        @NotNull String streamName
    ) {

        this.kinesisClient = kinesisClient;
        this.streamName = streamName;

        validateStream().join();
    }
}
