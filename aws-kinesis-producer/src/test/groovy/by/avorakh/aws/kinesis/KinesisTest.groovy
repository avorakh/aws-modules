package by.avorakh.aws.kinesis

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.KinesisException
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse
import software.amazon.kinesis.common.KinesisClientUtil
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class KinesisTest extends Specification {

    @Shared
    LocalStackContainer localstack

    def setupSpec() {

        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.11.3"))
            .withServices(LocalStackContainer.Service.KINESIS)
        localstack.start()
    }

    def cleanupSpec() {

        localstack.stop()
    }

    def "should get Region from localstack"() {

        when:
            Region actucalRegion = Region.of(localstack.getRegion())
        then:
            actucalRegion != null
            println(actucalRegion)
    }

    def "should get endpointOverride from localstack for KINESIS service"() {

        when:
            URI actualEndpointOverride = localstack.getEndpointOverride(LocalStackContainer.Service.KINESIS)
        then:
            actualEndpointOverride != null
            println(actualEndpointOverride)
    }

    def "should get credentials from localstack"() {

        when:
            String actualAccessKey = localstack.getAccessKey()
        then:
            actualAccessKey != null
            println(actualAccessKey)
        when:
            String actualSecretKey = localstack.getSecretKey()
        then:
            actualSecretKey != null
            println(actualSecretKey)
    }

    def "should create asynce http client"() {

        when:
            SdkAsyncHttpClient httpClient = getAsynceTestHttpClient()
        then:
            httpClient != null
            httpClient.close()
    }

    def "should get Kinesis client"() {

        given:

            Region region = getRegion(localstack)
            URI endpointOverride = getEndpointOverride(localstack, LocalStackContainer.Service.KINESIS)
            AwsCredentialsProvider credentialsProvider = getCredentialsProvider(localstack)
        when:

            KinesisAsyncClient kinesisClient = getKinesisClient(
                credentialsProvider,
                region,
                endpointOverride
            )
        then:
            kinesisClient != null
        and:
                        CreateStreamRequest createStreamRequest =  CreateStreamRequest.builder()
                            .streamName("some_stream_2")
                            .shardCount(1)
                            .build()
        when:
            ListStreamsResponse response = kinesisClient.listStreams().join()
        then:
            response != null
            println(response.streamNames())
    }

    def "should get Kinesis client v2 "() {

        given:

            SdkAsyncHttpClient httpClient = toNettyClient()
            getAsynceTestHttpClient()

            Region region = getRegion(localstack)

            URI endpointOverride = getEndpointOverride(localstack, LocalStackContainer.Service.KINESIS)

            AwsCredentialsProvider credentialsProvider = getCredentialsProvider(localstack)
        when:

            KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .endpointOverride(endpointOverride)
                .httpClient(httpClient)
                .build()
        then:
            kinesisClient != null
    }

    private static URI getEndpointOverride(LocalStackContainer localstack, LocalStackContainer.Service service) {

        return localstack.getEndpointOverride(service)
    }

    private static Region getRegion(LocalStackContainer localstack) {

        return Region.of(localstack.getRegion())
    }

    private static SdkAsyncHttpClient getAsynceTestHttpClient() {

        return NettyNioAsyncHttpClient.builder()
            .maxConcurrency(100)
            .maxPendingConnectionAcquires(10_000)
            .build()
    }

    private static SdkAsyncHttpClient toNettyClient() {

        return NettyNioAsyncHttpClient.builder().maxConcurrency(Integer.MAX_VALUE)

        //            .http2Configuration(Http2Configuration.builder().initialWindowSize(INITIAL_WINDOW_SIZE_BYTES)
        //                .healthCheckPingPeriod(Duration.ofMillis(HEALTH_CHECK_PING_PERIOD_MILLIS)).build())
            .protocol(Protocol.HTTP1_1)
            .build()
    }

    private static AwsCredentialsProvider getCredentialsProvider(LocalStackContainer localstack) {

        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
        )
        return credentialsProvider
    }

    private static KinesisAsyncClient getKinesisClient(
        AwsCredentialsProvider credentialsProvider,
        Region region,
        URI endpointOverride
    ) {

        return KinesisClientUtil.createKinesisAsyncClient(
            KinesisClientUtil.adjustKinesisClientBuilder(
                KinesisAsyncClient.builder()
                    .credentialsProvider(credentialsProvider)
                    .region(region)
                    .endpointOverride(endpointOverride))
        )
    }

    private static KinesisAsyncClient getKinesisClient(
        Region region,
        URI endpointOverride
    ) {

        return KinesisClientUtil.createKinesisAsyncClient(
            KinesisClientUtil.adjustKinesisClientBuilder(
                KinesisAsyncClient.builder()
                    .region(region)
                    .endpointOverride(endpointOverride))
        )
    }

    private static void createStream(KinesisAsyncClient kinesisClient, String streamName) {

        try {
            CreateStreamRequest streamReq = CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(1)
                .build()
            kinesisClient.createStream(streamReq).orTimeout(5, TimeUnit.SECONDS).join()
        } catch (KinesisException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
