package by.avorakh.aws.kinesis.svc.impl

import by.avorakh.aws.kinesis.svc.KinesisManageTestService
import by.avorakh.aws.kinesis.svc.KinesisProducer
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException
import software.amazon.kinesis.common.KinesisClientUtil
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CompletionException

class SimpleKinesisProducerTest extends Specification {

    @Shared
    KinesisClient testKinesisClient
    @Shared
    KinesisManageTestService kinesisManageTestService

    @Shared
    Region region = Region.of("us-east-1")

    @Shared
    URI endpointOverride = URI.create("http://localhost:4566")

    @Shared
    String streamName = "TestStream"

    KinesisProducer producer

    def setupSpec() {

        //
        //        def region = Region.of("us-east-1")
        //
        //        def endpointOverride = URI.create("http://localhost:4566")

        testKinesisClient = KinesisClient.builder()
            .httpClient(ApacheHttpClient.create())
            .region(region)
            .endpointOverride(endpointOverride)
            .build()
        kinesisManageTestService = new DefaultKinesisManageService(testKinesisClient)
        kinesisManageTestService.deleteAllStreams()
    }

    def setup() {

        kinesisManageTestService.prepareStream(streamName, 1)
    }

    def cleanupSpec() {

        //  kinesisManageTestService.deleteAllStreams()
        testKinesisClient.close()
    }

    def 'should successfully create Producer'() {

        when:
            producer = new SimpleKinesisProducer(getKinesisClient(region, endpointOverride), streamName)
        then:
            producer != null
    }

    def 'should thrown exception by connecting to inexistent stream'() {

        when:
            new SimpleKinesisProducer(getKinesisClient(region, endpointOverride), "inexistentSteam")
        then:
            thrown(CompletionException) // TODO modify for exception
    }

    def 'should successfully sent a record with Producer'() {

        given:
            producer = new SimpleKinesisProducer(getKinesisClient(region, endpointOverride), streamName)
            def key = "SomeKey"

            def data = "TestData"
        when:
            producer.putRecord(key, data.getBytes()).join()
            testKinesisClient.getShardIterator()
            testKinesisClient.getRecords()

        then:
            noExceptionThrown()
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
