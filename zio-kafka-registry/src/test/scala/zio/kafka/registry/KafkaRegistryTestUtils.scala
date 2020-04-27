package zio.kafka.registry

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka}
import org.apache.kafka.clients.consumer.ConsumerConfig
import zio.{Chunk, ZIO, Has, RIO}
import org.apache.kafka.clients.producer.ProducerRecord
import zio.blocking.Blocking
import zio.clock.Clock
// import zio.kafka.client.serde.{Serde, Serializer}
import zio.kafka.serde.{Serde, Serializer, Deserializer}
import zio.duration._
// import zio.kafka.client.KafkaAdmin.KafkaAdminClientConfig
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.kafka.producer.{ProducerSettings, Producer}
import zio.kafka.consumer.{ConsumerSettings, Consumer}
// import zio.random.Random
// import zio.test.environment.TestEnvironment
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
// import zio.kafka.registry.kafka.Kafka.KafkaTestEnvironment
// import zio.kafka.registry.Settings.RecordNameStrategy
// import zio.test.TestFailure

import scala.reflect.ClassTag
import zio.kafka.registry.confluentClientService.ConfluentClientService
import zio.ZLayer
import izumi.reflect.Tags.Tag

package object kafka {

  import zio.Has
  type Kafka = Has[Kafka.Service]
  object Kafka {
    trait Service {
      def bootstrapServers: List[String]
      def registryServer: String
      def stop(): ZIO[Any, Nothing, Unit]
    }

    case class EmbeddedKafkaService(embeddedK: EmbeddedKWithSR) extends Kafka.Service {
      private val kafkaPort = 6001
      val schemaRegistryPort = 6002
      override def bootstrapServers: List[String] = List(s"localhost:$kafkaPort")
      override def registryServer: String = s"http://localhost:$schemaRegistryPort"
      override def stop(): ZIO[Any, Nothing, Unit]        = ZIO.effectTotal(embeddedK.stop(true))
    }
    case object DefaultLocal extends Kafka.Service {
      override def bootstrapServers: List[String] = List(s"http://localhost:9092")
      override def registryServer: String = s"http://localhost:3000"

      override def stop(): ZIO[Any, Nothing, Unit] = ZIO.unit
    }

    def makeEmbedded: ZLayer[Any, Nothing, Kafka] = ZLayer.succeed({
      println("making -----")
      EmbeddedKafkaService(EmbeddedKafka.start())
    }
    )

    def makeLocal: ZLayer[Any, Nothing, Kafka] = ZLayer.succeed(
      DefaultLocal
    )

    type KafkaTestEnvironment = Kafka with ConfluentClientService

  }
}



package object kafkaRegistryTestUtils {

import zio.kafka.registry.Settings.TopicNameStrategy

  import org.apache.kafka.clients.producer.RecordMetadata
  type KafkaRegistryTestUtils = Has[KafkaRegistryTestUtils.Service]

  object KafkaRegistryTestUtils{
    trait Service{
      def registryProducerSettings: ZIO[Any, Nothing, ProducerSettings]

      def consumerSettings(groupId: String, clientId: String): ZIO[Any, Nothing, ConsumerSettings]

      def withConsumer[A : ClassTag, K, V](kSerde: Deserializer[Any, K],
                                vSerde: Deserializer[Any with Blocking with Clock, V],
                                groupId: String,
                                clientId: String)(
      r: Consumer[Any with Blocking with Clock, K, V] => RIO[Any with Clock with Blocking, A])
      (implicit ktag: Tag[K], vtag: Tag[V]): ZIO[Blocking with Clock, Throwable, A]

      def withProducer[A,K,V](kSerde: Serializer[Any,K],
                                vSerde: Serializer[Any with Blocking, V]
                           )(r: Producer[Any with Blocking, K, V] => RIO[Blocking, A])
                           (implicit ktag: Tag[K], vtag: Tag[V]): ZIO[Blocking, Throwable, A]

      // def withProducerAvroRecord[A, V](recordFormat: RecordFormat[V])
      def withProducerAvroRecord[A]
                                  (r: Producer[Any with Blocking, String, GenericRecord] => RIO[Blocking, A]
                                  ): ZIO[Blocking, Throwable, A]

      def produceMany[T](t: String, kvs: Iterable[(String, T)])
                    (implicit recordFormat: RecordFormat[T]): ZIO[Blocking, Throwable, Chunk[RecordMetadata]]
      def stop(): ZIO[Any, Nothing, Unit]

      def adminSettings: ZIO[Any, Nothing, AdminClientSettings]

      def withAdmin[T](f: AdminClient => RIO[Any with Clock with Blocking, T]): ZIO[Any with Clock with Blocking, Throwable, T]

    }
    def live = ZLayer.fromServices[kafka.Kafka.Service, ConfluentClientService.Service, KafkaRegistryTestUtils.Service]((k: kafka.Kafka.Service, ccs: ConfluentClientService.Service) =>
    new Service {
      def registryProducerSettings = ZIO.succeed(ProducerSettings(k.bootstrapServers, 5.seconds, Map.empty)
      .withProperty("acks", "all")
      // .withProperty("sasl.mechanism", "PLAIN")
      // .withProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";")
      // .withProperty("security.protocol","SASL_PLAINTEXT")
      )

      def consumerSettings(groupId: String, clientId: String) = ZIO.succeed(ConsumerSettings(
        k.bootstrapServers)
        .withGroupId(groupId)
        .withClientId(clientId)
        .withCloseTimeout(5.seconds)
        .withPollInterval(250.millis)
        .withPollTimeout(250.millis)
        .withProperties(
          Map(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
            ConsumerConfig.METADATA_MAX_AGE_CONFIG  -> "100",
          ))
        .withProperty("acks", "all")
        // .withProperty("sasl.mechanism", "PLAIN")
        // .withProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";")
        // .withProperty("security.protocol","SASL_PLAINTEXT")
        )


      def withConsumer[A : ClassTag, K, V](kSerde: Deserializer[Any, K],
                                vSerde: Deserializer[Any with Blocking with Clock, V],
                                groupId: String,
                                clientId: String)(
      r: Consumer[Any with Blocking with Clock, K, V] => RIO[Any with Clock with Blocking, A])
      (implicit ktag: Tag[K], vtag: Tag[V]) =
      for {
        settings <- consumerSettings(groupId, clientId)
        consumer = Consumer.make(settings, kSerde, vSerde)
        consumed <- consumer.build.use(p => r(p))
      } yield consumed

      def withProducer[A, K, V](kSerde: Serializer[Any, K],
                                vSerde: Serializer[Any with Blocking, V]
                           )(r: Producer[Any with Blocking, K, V] => RIO[Blocking, A])
                           (implicit ktag: Tag[K], vtag: Tag[V]) =
      for {
        settings <- registryProducerSettings
        producer = Producer.make(settings, kSerde, vSerde)
        produced <- producer.build.use ( p => r(p))
      } yield produced

      def withProducerAvroRecord[A]
                                  (r: Producer[Any with Blocking, String, GenericRecord] => RIO[Blocking, A]
                                  ) =
      for {
        serializer <- ccs.avroSerializer(TopicNameStrategy)
        producing <-  withProducer(Serde.string, serializer)(r)
      } yield producing

    def produceMany[T](t: String, kvs: Iterable[(String, T)])
                    (implicit recordFormat: RecordFormat[T]) =
    // withProducerAvroRecord(recordFormat) { p =>
    withProducerAvroRecord { p =>
      val records = kvs.map {
        case (k, v) =>
          val rec = recordFormat.to(v)
          new ProducerRecord[String, GenericRecord](t, k, rec)
      }
      val chunk = Chunk.fromIterable(records)
      p.get.produceChunk(chunk)
    }.flatten

    def stop() = k.stop()

    def adminSettings = ZIO.succeed(AdminClientSettings(k.bootstrapServers))

    def withAdmin[T](f: AdminClient => RIO[Any with Clock with Blocking, T]) =
    for {
      settings <- adminSettings
      fRes <- AdminClient
              .make(settings)
              .use{client => f(client)}
    } yield fRes

    // // temporary workaround for zio issue #2166 - broken infinity
    // val veryLongTime = Duration.fromNanos(Long.MaxValue)

    // def randomThing(prefix: String) =
    //   for {
    //     random <- ZIO.environment[Random]
    //     l      <- random.random.nextLong(8)
    //   } yield s"$prefix-$l"

    // def randomTopic = randomThing("topic")

    // def randomGroup = randomThing("group")
    }
    )
    def produceMany[T](t: String, kvs: Iterable[(String, T)])
                      (implicit recordFormat: RecordFormat[T]) = ZIO.accessM[KafkaRegistryTestUtils with Blocking](_.get.produceMany(t, kvs)(recordFormat))

    def withConsumer[A : ClassTag, K, V](kSerde: Deserializer[Any, K],
                                vSerde: Deserializer[Any with Blocking with Clock, V],
                                groupId: String,
                                clientId: String)(
      r: Consumer[Any with Blocking with Clock, K, V] => RIO[Any with Clock with Blocking, A])
      (implicit ktag: Tag[K], vtag: Tag[V]) = ZIO.accessM[KafkaRegistryTestUtils with Blocking with Clock](_.get.withConsumer(kSerde,vSerde,groupId,clientId)(r))
    def stop = ZIO.accessM[KafkaRegistryTestUtils](_.get.stop())

  }

}

import zio.test.{suite, DefaultRunnableSpec, testM}
import zio.test.TestAspect.sequential
import zio.test.assert
import zio.test.Assertion._
object AllSuites extends DefaultRunnableSpec {
  import zio.kafka.registry.confluentRestServiceReq.ConfluentRestServiceReq
  import zio.kafka.registry.confluentRestService.ConfluentRestService
  import zio.kafka.registry.kafkaRegistryTestUtils.KafkaRegistryTestUtils
  import TestRestConfluent.restSuite
  import TestProducerSupport.prodConSuite

  val crlr = ConfluentRestServiceReq.live
  val crl = crlr >>> ConfluentRestService.live("http://localhost:6002")
  val ccsLayer = (crl ++crlr ++ Blocking.live) >>> ConfluentClientService.live("http://localhost:6002", 1000)
  val kafkaLayer = kafka.Kafka.makeEmbedded
  val krtuLayer = (kafkaLayer ++ ccsLayer) >>> KafkaRegistryTestUtils.live


  // Work around to calling kafkaService.stop so tests release kafka upon completion
  // Made tests run sequentially and this suite must always be last
  val clearUp = suite("Release resources")(
    testM("Release Kafka"){
      for {
        _ <- kafkaRegistryTestUtils.KafkaRegistryTestUtils.stop
      } yield assert(true)(equalTo(true))
    }
  ) @@ sequential
  def spec = {
    suite("All Tests")(restSuite, prodConSuite, clearUp).provideLayerShared(krtuLayer ++ Blocking.live ++ Clock.live ++ ccsLayer) @@ sequential
  }
}