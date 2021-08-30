package by.avorakh.aws.kinesis.svc.impl

import by.avorakh.aws.kinesis.KinesisStreamStatus
import by.avorakh.aws.kinesis.svc.KinesisManageService
import by.avorakh.aws.kinesis.svc.KinesisManageTestService
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
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

        testKinesisClient = KinesisClient.builder()
            .httpClient(ApacheHttpClient.create())
            .region(region)
            .endpointOverride(endpointOverride)
            .build()
        kinesisManageTestService = new DefaultKinesisManageService(testKinesisClient)
    }

    def setup() {

        kinesisManageService = new DefaultKinesisManageService(testKinesisClient)
        kinesisManageTestService.deleteAllStreams()
    }

    def cleanupSpec() {

        testKinesisClient.close()
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
