package by.avorakh.aws.kinesis.svc;

import by.avorakh.aws.kinesis.PutRecordPub;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public interface AsyncKinesisProducer {

    @NotNull CompletableFuture<PutRecordPub> putRecord(@NotNull String partitionKey, byte @NotNull [] bytes);
}
