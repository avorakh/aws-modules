package by.avorakh.aws.kinesis.svc;

import by.avorakh.aws.kinesis.KinesisStreamStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.util.List;

public interface KinesisStreamInformater {

    @NotNull List<String> getStreamNames();

    @NotNull KinesisStreamStatus getStreamStatus(@NotNull String streamName);

    @Nullable StreamDescription getStreamInfo(@NotNull String streamName);
}
