package by.avorakh.aws.kinesis.svc.impl;

import by.avorakh.aws.kinesis.KinesisStreamStatus;
import by.avorakh.aws.kinesis.svc.KinesisManageService;
import by.avorakh.aws.kinesis.svc.KinesisManageTestService;
import by.avorakh.aws.kinesis.util.KinesisRequestUtil;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

@Slf4j
public class DefaultKinesisManageService extends SimpleKinesisStreamInformater implements KinesisManageService,
    KinesisManageTestService {

    @Override
    public @NotNull KinesisStreamStatus createStream(@NotNull String streamName, int shardCount) {

        try {

            var streamStreamStatus = getStreamStatus(streamName);
            log.warn("Stream '{}', status: '{}'.", streamName, streamStreamStatus);

            if (streamStreamStatus == KinesisStreamStatus.NOT_FOUND) {
                var createStreamRequest = KinesisRequestUtil.createStreamRequest(streamName, shardCount);

                kinesisClient.createStream(createStreamRequest);

                log.warn("Stream '{}' is creating", streamName);
                return KinesisStreamStatus.CREATING;
            }
            return streamStreamStatus;
        } catch (KinesisException e) {
            log.error("Impossible create  Kinesis stream by:", e);
            throw e;
        }
    }

    @Override
    public void prepareStream(@NotNull String streamName, int shardCount) throws InterruptedException {

        KinesisStreamStatus streamStatus;

        do {
            streamStatus = createStream(streamName, shardCount);
        } while (!KinesisStreamStatus.ACTIVE.equals(streamStatus));

        Thread.sleep(100);

        log.warn("Stream '{}' is prepared", streamName);
    }

    @Override
    public @NotNull KinesisStreamStatus deleteStream(@NotNull String streamName) {

        try {

            var currentStreamStatus = getStreamStatus(streamName);

            if (!KinesisStreamStatus.ACTIVE.equals(currentStreamStatus)) {

                log.error("Stream '{}', status: '{}'.", streamName, currentStreamStatus);
                return currentStreamStatus;
            }

            var deleteStreamRequest = KinesisRequestUtil.toDeleteStreamRequest(streamName);
            kinesisClient.deleteStream(deleteStreamRequest);

            return KinesisStreamStatus.DELETING;
        } catch (KinesisException e) {
            log.error("Impossible delete Kinesis stream by:", e);
            throw e;
        }
    }

    @Override
    public synchronized void deleteAllStreams() throws InterruptedException {

        var streamNames = getStreamNames();

        if (streamNames.isEmpty()) {
            log.warn("There are not any streams");
            return;
        }

        do {
            log.warn("Stream count is '{}'", streamNames.size());
            for (var streamName : streamNames) {
                var streamStatus = deleteStream(streamName);
                log.warn("Stream '{}', status: '{}'.", streamName, streamStatus);
            }

            streamNames = getStreamNames();
        } while (!streamNames.isEmpty());

        Thread.sleep(100);
        log.warn("All streams are deleted");
    }

    public DefaultKinesisManageService(@NotNull KinesisClient kinesisClient) {

        super(kinesisClient);
    }
}
