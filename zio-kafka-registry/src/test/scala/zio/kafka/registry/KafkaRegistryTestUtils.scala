package zio.kafka.registry

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka}
import org.apache.kafka.clients.consumer.ConsumerConfig
import zio.{Cause, Chunk, Managed, RIO, UIO, ZIO, Has, IO}
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

import zio.test.{suite, DefaultRunnableSpec}
import zio.test.TestAspect.sequential

package object kafka {

  import zio.Has
  type Kafka = Has[Kafka.Service]
  object Kafka {
    trait Service {
      def bootstrapServers: List[String]
      def registryServer: String
      def stop(): UIO[Unit]
    }

    case class EmbeddedKafkaService(embeddedK: EmbeddedKWithSR) extends Kafka.Service {
      private val kafkaPort = 6001
      val schemaRegistryPort = 6002
      override def bootstrapServers: List[String] = List(s"localhost:$kafkaPort")
      override def registryServer: String = s"http://localhost:$schemaRegistryPort"
      override def stop(): UIO[Unit]              = ZIO.effectTotal(embeddedK.stop(true))
    }
    case object DefaultLocal extends Kafka.Service {
      override def bootstrapServers: List[String] = List(s"kafka.focaldata.dev:9092")
      override def registryServer: String = s"http://schemaregistry.focaldata.dev"

      override def stop(): UIO[Unit] = UIO.unit
    }

    def makeEmbedded: ZLayer[Any, Nothing, Kafka] = ZLayer.succeed(
      EmbeddedKafkaService(EmbeddedKafka.start())
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
  }



  // def adminSettings =
  //   for {
  //     servers <- ZIO.access[kafka.Kafka.Service](_.bootstrapServers)
  //   } yield AdminClientSettings(servers)

  // def withAdmin[T](f: AdminClient => RIO[Any with Clock with kafka.Kafka with Blocking, T]) =
  //   for {
  //     settings <- adminSettings
  //     fRes <- AdminClient
  //              .make(settings)
  //              .use { client =>
  //                f(client)
  //              }
  //   } yield fRes

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

object AllSuites extends DefaultRunnableSpec {
  import TestRestConfluent.restSuite
  import TestProducerSupport.prodConSuite

  def spec = {
    val kafkaLayer = kafka.Kafka.makeEmbedded
    suite("All Tests")(restSuite, prodConSuite(kafkaLayer))
  }
}