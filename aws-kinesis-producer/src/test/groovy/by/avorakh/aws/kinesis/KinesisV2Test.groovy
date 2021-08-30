package by.avorakh.aws.kinesis

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.StreamStatus
import software.amazon.kinesis.common.KinesisClientUtil
import spock.lang.Shared
import spock.lang.Specification

class KinesisV2Test extends Specification {

    @Shared
    KinesisAsyncClient kinesisClient

    @Shared
    KinesisClient testKinesisClient

    def setupSpec() {

        Region region = Region.of("us-east-1")

        URI endpointOverride = URI.create("http://localhost:4566")

        kinesisClient = getKinesisClient(
            region,
            endpointOverride
        )

        testKinesisClient = KinesisClient.builder()
            .httpClient(ApacheHttpClient.create())
            .region(region)
            .endpointOverride(endpointOverride)
            .build()
    }

    def "should successfully create stream with sync client"() {

        given:
            cleanStreams(testKinesisClient)

            def streamName = "TestStream"
            def createStreamRequest = createStreamRequest(streamName)
        when:
            testKinesisClient.createStream(createStreamRequest)
        then:
            noExceptionThrown()
    }

    def "should successfully create stream with async client"() {

        given:
            cleanStreams(testKinesisClient)

            def streamName = "TestStream2"
            def createStreamRequest = createStreamRequest(streamName)
        when:
            kinesisClient.createStream(createStreamRequest).join()
        then:
            noExceptionThrown()
    }

    private CreateStreamRequest createStreamRequest(String streamName) {

        return CreateStreamRequest.builder()
            .streamName(streamName)
            .shardCount(1)
            .build()
    }

    //    def "should get Kinesis client v2 "() {
    //
    //        given:
    //
    //            SdkAsyncHttpClient httpClient = toNettyClient()
    //            //getAsynceTestHttpClient()
    //
    //            Region region = getRegion(localstack)
    //
    //            URI endpointOverride = getEndpointOverride(localstack, LocalStackContainer.Service.KINESIS)
    //
    //            AwsCredentialsProvider credentialsProvider = getCredentialsProvider(localstack)
    //        when:
    //
    //            KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder()
    //                .credentialsProvider(credentialsProvider)
    //                .region(region)
    //                .endpointOverride(endpointOverride)
    //                .httpClient(httpClient)
    //                .build()
    //        then:
    //            kinesisClient != null
    //            //        and:
    //            ////            CreateStreamRequest createStreamRequest = CreateStreamRequest.builder()
    //            ////                .streamName("some_stream")
    //            ////                .shardCount(1)
    //            ////                .build()
    //            //            kinesisClient.createStream(createStreamRequest).thenApply(response -> {
    //            //                println(response)
    //            //            }).orTimeout(10, TimeUnit.SECONDS).join()
    //    }

    //    def "used client"() {
    //
    //        given:
    //
    //            Region region = Region.of("us-east-1")
    //
    //            URI endpointOverride = URI.create("http://localhost:4566")
    //
    //            //            AwsCredentialsProvider credentialsProvider = getCredentialsProvider(localstack)
    //        when:
    //
    //            KinesisClient kinesisClient = KinesisClient.builder()
    //                .httpClient(ApacheHttpClient.create())
    //                .region(region)
    //                .endpointOverride(endpointOverride)
    //            //                .credentialsProvider(credentialsProvider)
    //                .build()
    //        then:
    //            kinesisClient != null
    //        and:
    //            CreateStreamRequest createStreamRequest = CreateStreamRequest.builder()
    //                .streamName("some_stream")
    //                .shardCount(1)
    //                .build()
    //        when:
    //            CreateStreamResponse response = kinesisClient.createStream(createStreamRequest)
    //        then:
    //            response != null
    //            println(response)
    //    }

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

    private static AwsCredentialsProvider getCredentialsProvider(String accessKey, String secretKey) {

        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey))
    }



    static KinesisAsyncClient getKinesisClient(
        //AwsCredentialsProvider credentialsProvider,
        Region region,
        URI endpointOverride
    ) {

        return KinesisClientUtil.createKinesisAsyncClient(
            KinesisClientUtil.adjustKinesisClientBuilder(
                KinesisAsyncClient.builder()
                //                    .credentialsProvider(credentialsProvider)
                    .region(region)
                    .endpointOverride(endpointOverride))
        )
    }

    //    private static KinesisAsyncClient getKinesisClient(
    //        Region region,
    //        URI endpointOverride
    //    ) {
    //
    //        return KinesisClientUtil.createKinesisAsyncClient(
    //            KinesisClientUtil.adjustKinesisClientBuilder(
    //                KinesisAsyncClient.builder()
    //                    .region(region)
    //                    .endpointOverride(endpointOverride))
    //        )
    //    }

    private static void cleanStreams(KinesisClient testClient) {

        testClient.listStreams()
            .streamNames().
            forEach(
                streamName -> {
                    def describeStreamRequest = toDescribeStreamRequest(streamName)
                    def response = testClient.describeStream(describeStreamRequest)
                    println(response)
                    testClient.deleteStream(toDeleteStreamRequest(streamName))
                }
            )
    }

    private static DescribeStreamRequest toDescribeStreamRequest(String streamName) {

        return DescribeStreamRequest.builder()
            .streamName(streamName)
            .build()
    }

    private static DeleteStreamRequest toDeleteStreamRequest(String streamName) {

        return DeleteStreamRequest.builder()
            .streamName(streamName)
            .build()
    }
}
