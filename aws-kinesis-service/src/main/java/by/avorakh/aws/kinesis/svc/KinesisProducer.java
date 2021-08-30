package by.avorakh.aws.kinesis.svc;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public interface KinesisProducer {

   @NotNull CompletableFuture<Void> putRecord(@NotNull String partitionKey, byte @NotNull [] bytes);

//    void putReconds();
}
