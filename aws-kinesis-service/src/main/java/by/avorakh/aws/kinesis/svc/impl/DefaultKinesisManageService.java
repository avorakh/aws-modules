package by.avorakh.aws.kinesis.svc.impl;

import by.avorakh.aws.kinesis.KinesisStreamStatus;
import by.avorakh.aws.kinesis.svc.KinesisManageService;
import by.avorakh.aws.kinesis.svc.KinesisManageTestService;
import by.avorakh.aws.kinesis.util.KinesisRequestUtil;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class DefaultKinesisManageService implements KinesisManageService, KinesisManageTestService {

    @NotNull KinesisClient kinesisClient;

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
    public @NotNull List<String> getStreamNames() {

        return kinesisClient.listStreams().streamNames();
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

    private @NotNull KinesisStreamStatus getStreamStatus(@NotNull String streamName) {

        var streamDescription = getStreamInfo(streamName);

        if (streamDescription == null) {
            return KinesisStreamStatus.NOT_FOUND;
        }

        switch (streamDescription.streamStatus()) {
            case CREATING:
                return KinesisStreamStatus.CREATING;
            case DELETING:
                return KinesisStreamStatus.DELETING;

            case UPDATING:
                return KinesisStreamStatus.UPDATING;

            case ACTIVE:
            default:
                return KinesisStreamStatus.ACTIVE;
        }
    }

    @Override
    public @Nullable StreamDescription getStreamInfo(@NotNull String streamName) {

        try {

            if (!containStream(streamName)) {

                log.error("Stream '{}' does not exist.", streamName);
                return null;
            }

            var describeStreamRequest = KinesisRequestUtil.toDescribeStreamRequest(streamName);
            var describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);
            return describeStreamResponse.streamDescription();
        } catch (KinesisException e) {
            log.error("Impossible get info about Kinesis stream by:", e);
            throw e;
        }
    }

    @Override
    public void deleteAllStreams() throws InterruptedException {

        var streamNames = getStreamNames();

        if (streamNames.isEmpty()) {
            log.warn("There are not any streams");
            return;
        }

        while (!streamNames.isEmpty()) {
            log.warn("Stream count is '{}'", streamNames.size());
            for (var streamName : streamNames) {
                var streamStatus = deleteStream(streamName);
                log.warn("Stream '{}', status: '{}'.", streamName, streamStatus);
            }

            streamNames = getStreamNames();
        }

        Thread.sleep(100);
        log.warn("All streams are deleted");
    }

    private boolean containStream(@NotNull String streamName) {

        return getStreamNames().contains(streamName);
    }
}
