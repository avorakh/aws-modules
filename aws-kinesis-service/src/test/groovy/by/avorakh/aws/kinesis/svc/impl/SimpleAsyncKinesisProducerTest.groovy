package by.avorakh.aws.kinesis.svc.impl

import by.avorakh.aws.kinesis.svc.AsyncKinesisProducer
import by.avorakh.aws.kinesis.svc.KinesisConsumer
import by.avorakh.aws.kinesis.svc.KinesisManageTestService
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.kinesis.common.KinesisClientUtil
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CompletionException

class SimpleAsyncKinesisProducerTest extends Specification {

    @Shared
    KinesisClient testKinesisClient
    @Shared
    KinesisManageTestService kinesisManageTestService
    @Shared
    KinesisConsumer kinesisConsumer

    @Shared
    Region region = Region.of("us-east-1")

    @Shared
    URI endpointOverride = URI.create("http://localhost:4566")

    @Shared
    AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
        AwsBasicCredentials.create("AccessKey", "SecretKey")
    )

    @Shared
    String streamName = "TestStream"

    AsyncKinesisProducer producer

    def setupSpec() {

        testKinesisClient = KinesisClient.builder()
            .httpClient(ApacheHttpClient.create())
            .credentialsProvider(credentialsProvider)
            .region(region)
            .endpointOverride(endpointOverride)
            .build()

        kinesisManageTestService = new DefaultKinesisManageService(testKinesisClient)

        kinesisManageTestService.deleteAllStreams()
    }

    def setup() {

        kinesisManageTestService.prepareStream(streamName, 1)

        Thread.sleep(500)

        kinesisConsumer = new SimpleSimpleKinesisConsumer(testKinesisClient, streamName)
    }

    def cleanup() {

        kinesisManageTestService.deleteAllStreams()
    }

    def cleanupSpec() {

        testKinesisClient.close()
    }

    def 'should successfully create Producer'() {

        when:
            producer = new SimpleAsyncKinesisProducer(getKinesisClient(region, endpointOverride), streamName)
        then:
            producer != null
    }

    def 'should thrown exception by connecting to inexistent stream'() {

        when:
            new SimpleAsyncKinesisProducer(getKinesisClient(region, endpointOverride), "inexistentSteam")
        then:
            thrown(CompletionException) // TODO modify for exception
    }

    def 'should successfully sent a record with Producer'() {

        given:
            producer = new SimpleAsyncKinesisProducer(getKinesisClient(region, endpointOverride), streamName)
            def partitionKey = "SomeKey"
            def data_base64_bytes = "IHsiZGV2aWNlWGlkIjoiUUEtMjcwMC0xMjEtMTM0MzQwMzY2IiwiY29ubmVjdGVkIjp0cnVlLCJ0aW1lc3RhbXAiOjE2MzAzMTA4NDh9".getBytes()
        when:
            def putRecordPub = producer.putRecord(partitionKey, data_base64_bytes).join()
        then:
            putRecordPub != null
            putRecordPub.putted
            putRecordPub.shardId != null
            putRecordPub.sequenceNumber != null
        and:
            def shardId = putRecordPub.shardId
            def sequenceNumber = putRecordPub.sequenceNumber
        when:
            def foundRecord = kinesisConsumer.getRecord(shardId, sequenceNumber)
        then:
            noExceptionThrown()
            foundRecord != null
            foundRecord.partitionKey == partitionKey
            foundRecord.bytes == data_base64_bytes
    }

    static KinesisAsyncClient getKinesisClient(
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
}
