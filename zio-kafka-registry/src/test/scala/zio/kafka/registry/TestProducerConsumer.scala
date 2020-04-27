package zio.kafka.registry

import zio.test._
import TestRestSupport._
import kafkaRegistryTestUtils.KafkaRegistryTestUtils
import zio.test.TestAspect._
import Assertion._
import zio.kafka.consumer.Subscription
import zio.kafka.serde.Serde
import zio.kafka.registry.Settings.{TopicNameStrategy}
import zio.kafka.registry.confluentClientService.ConfluentClientService

object TestProducerSupport{
  val topic = "presidents"

  val presidents = List(President2("Lincoln", 1860),
  President2("Obama", 2008),
  President2("Trump", 2016))
  def testPresident1 = testM("test define and store president") {
    for {
      _ <- KafkaRegistryTestUtils.produceMany(topic, presidents.map (p => "all" -> p))(format2)
      subjects <- ConfluentClientService.subjects
      deserializer <- ConfluentClientService.avroGenericDeserializer[President2](TopicNameStrategy)(format2)
      records <- KafkaRegistryTestUtils.withConsumer(Serde.string, deserializer,"any", "client") { consumer =>
        consumer.get
        .subscribeAnd(Subscription.Topics(Set(topic)))
        .plainStream
        .flattenChunks
        .take(3)
        .runCollect
      }
      vOut = records.map{_.record.value}
    } yield {
      println(s"got these subjects $subjects")
      println(vOut)
      assert(presidents)(equalTo(presidents))
    }
  }

  def prodConSuite = {
    suite("Testing of producer and consumers with avro")(testPresident1) @@ sequential
  }
}
