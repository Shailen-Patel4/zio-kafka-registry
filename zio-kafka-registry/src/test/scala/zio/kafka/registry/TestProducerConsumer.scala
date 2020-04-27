package zio.kafka.registry

import zio.test._
import TestRestSupport._
// import KafkaRegistryTestUtils._
import kafkaRegistryTestUtils.KafkaRegistryTestUtils
import kafka.Kafka
import zio._
import zio.test.TestAspect._
import TestProducerSupport._
import Assertion._
import zio.kafka.consumer.Subscription
import zio.kafka.serde.Serde
import zio.kafka.registry.Settings.{RecordNameStrategy, TopicNameStrategy}
import zio.kafka.registry.confluentClientService.ConfluentClientService
import zio.kafka.registry.confluentRestService.ConfluentRestService
import zio.kafka.registry.confluentRestServiceReq.ConfluentRestServiceReq
import zio.blocking.Blocking
import zio.clock.Clock
import com.sksamuel.avro4s.RecordFormat
// object TestProducerConsumer extends DefaultRunnableSpec {
//   def spec = suite("test producing with avro serializer")(
//     testPresident
//   ) @@ sequential
// }

object TestProducerSupport{


  val topic = "presidents"

  val presidents = List(President2("Lincoln", 1860),
  President2("Obama", 2008),
  President2("Trump", 2016))

  def testPresident(kafkaLayer: ZLayer[Any, Nothing, kafka.Kafka]) = testM("test define and store president") {
    val crlr = ConfluentRestServiceReq.live
    val crl = crlr >>> ConfluentRestService.live("http://schemaregistry.focaldata.dev")
    val ccsLayer = (crl ++crlr ++ Blocking.live) >>> ConfluentClientService.live("http://schemaregistry.focaldata.dev", 1000)
    // val kLayer = Kafka.makeEmbedded
    val krtuLayer = (kafkaLayer ++ ccsLayer) >>> KafkaRegistryTestUtils.live
    for {
      _ <- KafkaRegistryTestUtils.produceMany(topic, presidents.map (p => "all" -> p))(format2).provideLayer(krtuLayer ++ Blocking.live)
      subjects <- ConfluentClientService.subjects.provideLayer(ccsLayer)
      deserializer <- ConfluentClientService.avroGenericDeserializer[President2](TopicNameStrategy)(format2).provideLayer(ccsLayer)
      records <- KafkaRegistryTestUtils.withConsumer(Serde.string, deserializer,"any", "client") { consumer =>
        consumer.get
        .subscribeAnd(Subscription.Topics(Set(topic)))
        .plainStream
        .flattenChunks
        .take(3)
        .runCollect
      }.provideLayer(krtuLayer ++ Blocking.live ++ Clock.live)
      vOut = records.map{_.record.value}
    } yield {
      println(s"got these subjects $subjects")
      println(vOut)
      assert(presidents)(equalTo(presidents))
    }
  }

  def prodConSuite(kafkaLayer: ZLayer[Any, Nothing, kafka.Kafka]) = {
    suite("Testing of producer and consumers with avro")(testPresident(kafkaLayer)) @@ sequential
  }
}
