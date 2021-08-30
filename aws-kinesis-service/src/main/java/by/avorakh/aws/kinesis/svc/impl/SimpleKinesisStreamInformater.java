package by.avorakh.aws.kinesis.svc.impl;

import by.avorakh.aws.kinesis.KinesisStreamStatus;
import by.avorakh.aws.kinesis.svc.KinesisStreamInformater;
import by.avorakh.aws.kinesis.util.KinesisRequestUtil;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.util.List;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
public class SimpleKinesisStreamInformater implements KinesisStreamInformater {

    @NotNull KinesisClient kinesisClient;

    public @NotNull List<String> getStreamNames() {

        return kinesisClient.listStreams().streamNames();
    }

    protected boolean containStream(@NotNull String streamName) {

        return getStreamNames().contains(streamName);
    }

    public @NotNull KinesisStreamStatus getStreamStatus(@NotNull String streamName) {

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

    public @Nullable StreamDescription getStreamInfo(@NotNull String streamName) {

        try {

            if (!containStream(streamName)) {

                log.error("Stream '{}' does not exist.", streamName);
                return null;
            }

            var describeStreamRequest = KinesisRequestUtil.toDescribeStreamRequest(streamName);
            var describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);
            return describeStreamResponse.streamDescription();
        } catch (ResourceNotFoundException e) {
            log.error("Impossible get info about Kinesis stream: '{}'  by:", streamName, e);
            return null;
        } catch (KinesisException e) {
            log.error("Impossible get info about Kinesis stream: '{}'  by:", streamName, e);
            throw e;
        }
    }

    protected boolean validateStream(@NotNull String streamName) {

        var streamStatus = getStreamStatus(streamName);

        if (!KinesisStreamStatus.ACTIVE.equals(streamStatus)) {
            log.error(
                "Stream '{}' is not active (Status: '{}'). Please wait a few moments and try again.",
                streamName,
                streamStatus
            );
            return Boolean.FALSE;
        }
        log.debug("Stream '{}' is active.", streamName);
        return Boolean.TRUE;
    }
}
