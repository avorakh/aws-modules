package by.avorakh.aws.kinesis.svc.impl

import by.avorakh.aws.kinesis.KinesisStreamStatus
import by.avorakh.aws.kinesis.svc.KinesisManageService
import by.avorakh.aws.kinesis.svc.KinesisManageTestService
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import spock.lang.Retry
import spock.lang.Shared
import spock.lang.Specification

class DefaultKinesisManageServiceTest extends Specification {

    @Shared
    KinesisClient testKinesisClient
    @Shared
    KinesisManageTestService kinesisManageTestService

    KinesisManageService kinesisManageService

    def setupSpec() {

        def region = Region.of("us-east-1")

        def endpointOverride = URI.create("http://localhost:4566")

        def credentialsProvider = StaticCredentialsProvider.create(
            AwsBasicCredentials.create("AccessKey", "SecretKey")
        )

        testKinesisClient = KinesisClient.builder()
            .httpClient(ApacheHttpClient.create())
            .credentialsProvider(credentialsProvider)
            .region(region)
            .endpointOverride(endpointOverride)
            .build()
        kinesisManageTestService = new DefaultKinesisManageService(testKinesisClient)
    }

    def setup() {

        kinesisManageService = new DefaultKinesisManageService(testKinesisClient)
        kinesisManageTestService.deleteAllStreams()
    }

    def cleanup() {

        kinesisManageTestService.deleteAllStreams()
    }

    def cleanupSpec() {

        testKinesisClient.close()
    }

    def "should successfully delete all streams"() {

        when:
            kinesisManageTestService.deleteAllStreams()
        then:
            noExceptionThrown()
    }

    def "should successfully prepare stream"() {

        given:
            String streamName = "SomeTestStream0"
        when:
            kinesisManageTestService.prepareStream(streamName, 1)
        then:
            noExceptionThrown()
    }

    def "should successfully create stream"() {

        given:
            String streamName = "SomeTestStream1"
            int shardCount = 1
        when:
            def result = kinesisManageService.createStream(streamName, shardCount)
        then:
            result != null
            result == KinesisStreamStatus.CREATING
    }

    @Retry(count = 5)
    def "should successfully delete stream"() {

        given:
            String streamName = "SomeTestStream2"
            kinesisManageTestService.prepareStream(streamName, 1)
        when:
            def result = kinesisManageService.deleteStream(streamName)
        then:
            result == KinesisStreamStatus.DELETING
    }
}
