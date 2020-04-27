
package zio.kafka.registry

import io.confluent.kafka.schemaregistry.client.rest.entities.{Schema => ConfluentSchema}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro
import org.apache.avro.{Schema => AvroSchema}
import zio.blocking._
import Serializers._
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import zio.kafka.registry.AvroSerdes.{AvroDeserializer, AvroGenericDeserializer, AvroSerializer}
import zio.kafka.registry.Settings.SubjectNameStrategy
import zio.{IO, Task, ZIO}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import zio.ZLayer
import zio.kafka.registry.confluentRestService.ConfluentRestService
import zio.kafka.registry.confluentRestService.ConfluentRestService.{WrappedSchema, CompatibilityLevel, SchemaError}
import zio.Has

/**
 * Service to interface with schema
 */
package object confluentClientService {

import zio.kafka.registry.confluentRestServiceReq.ConfluentRestServiceReq

  type ConfluentClientService = Has[ConfluentClientService.Service]
  object ConfluentClientService {
    trait Service {
      type RestResponse[T] = ZIO[Any, Throwable, T]

      val url: String
      val confluentRestService: ConfluentRestService.Service
      protected[registry] val registryClient: CachedSchemaRegistryClient

      def restResponse[T](f: => T): RestResponse[T]

      def restResponseM[T](f: => Task[T]): RestResponse[T]

      def schema(id: Int): RestResponse[avro.Schema]

      def subjects: RestResponse[List[String]]

      def subjectVersions(subject: String): RestResponse[List[Int]]

      def deleteSubject(subject: String): RestResponse[List[Int]]

      def version(subject: String, versionId: Option[Int]): RestResponse[WrappedSchema]

      def registerSchema(subject: String, schema: avro.Schema): RestResponse[Int]

      def alreadyPresent(subject: String, schema: avro.Schema): RestResponse[Option[WrappedSchema]]

      def delete(subject: String, versionId: Int): RestResponse[Unit]

      def compatible(subject: String, versionId: Int, schema: avro.Schema): RestResponse[Boolean]

      def setConfig(compatibilityLevel: CompatibilityLevel): RestResponse[Unit]

      def config: RestResponse[CompatibilityLevel]

      def setConfig(subject: String, compatibilityLevel: CompatibilityLevel): RestResponse[Unit]

      def config(subject: String): RestResponse[CompatibilityLevel]

      def avroSerializer(subjectNameStrategy: SubjectNameStrategy,
                        additionalParams: Map[String, Any] = Map.empty): ZIO[Any,Throwable,AvroSerializer]

      def avroDeserializer[T : ClassTag](subjectNameStrategy: SubjectNameStrategy,
                          additionalParams: Map[String, Any] = Map.empty): ZIO[Any,Throwable,AvroDeserializer[T]]

      def avroGenericDeserializer[T](subjectNameStrategy: SubjectNameStrategy,
                                    additionalParams: Map[String, Any] = Map.empty)
                                    (recordFormat: RecordFormat[T]): ZIO[Any,Throwable,AvroGenericDeserializer[T]]

    }
    type RestResponse[T] = ZIO[Any, Throwable, T]
    def live(urlIn: String,
             identityMapCapacity: Int) = ZLayer.fromServices[ConfluentRestService.Service, ConfluentRestServiceReq.Service, Blocking.Service, ConfluentClientService.Service]{(rs: ConfluentRestService.Service, rsr: ConfluentRestServiceReq.Service, b: Blocking.Service) =>
    new Service {
      val url: String = urlIn
      val confluentRestService: ConfluentRestService.Service = rs
      val registryClient: CachedSchemaRegistryClient = new CachedSchemaRegistryClient(confluentRestService.jrs, identityMapCapacity)

      private def jrs = rs.jrs

      private def failSchemaError[T](exception: RestClientException): RestResponse[T] =
        IO.fail(SchemaError(exception.getErrorCode, exception.getMessage))

      private def checkSchemaError[T](ex: Throwable) = ex match {
        case e: RestClientException =>
          failSchemaError[T](e)
        case x => IO.fail(ex)
      }

      def restResponse[T](f: => T): RestResponse[T] = for {
        sem <- rsr.sem
        result <- sem.withPermit( b.blocking(
          try {
            IO.effectTotal(f)
          } catch {
            case e: Throwable => checkSchemaError(e)
          }))
        } yield result

      def restResponseM[T](f: => Task[T]): RestResponse[T] = for {
        sem <- rsr.sem
        result <- sem.withPermit(
          b.blocking {
            try {
              f
            } catch {
              case e: Throwable => checkSchemaError(e)
            }
          }
        )
      } yield result

      /**
       * gets schema for given id as avro.Schema
       * @param id
       * @return schema requested. Error if missing
       */
      def schema(id: Int): RestResponse[avro.Schema] = restResponseM {
        parseSchemaDirect(jrs.getId(id).getSchemaString)
      }

      /**
        * @return list of schema subject names
      */
      def subjects: RestResponse[List[String]] =
      restResponse { jrs.getAllSubjects.asScala.toList }

      /**
       * gets all version ids for given subject
       * @param subject
       * @return list of version numbers for given subject
       */
      def subjectVersions(subject: String): RestResponse[List[Int]] =
      restResponse {jrs.getAllVersions(subject).asScala.toList.map(_.toInt)}


      /**
       * delets subject with all versions. Confluent docs recommend only using this in testing!
       * @param subject
       * @return List of deleted version numbers
       */
      def deleteSubject(subject: String): RestResponse[List[Int]] =
      restResponse {jrs.deleteSubject(Map.empty[String, String].asJava, subject).asScala.toList.map(_.toInt)}

      private def parseWrapped(cs: ConfluentSchema): Task[WrappedSchema] =
        IO.effect {WrappedSchema(cs.getSubject, cs.getId, cs.getVersion, new AvroSchema.Parser().parse(cs.getSchema))}


      private def wrappedSchemaResponse(f: => ConfluentSchema): RestResponse[WrappedSchema] =
      restResponseM {parseWrapped(f)}


      /**
       * gets a specific schema
       * @param subject subject name
       * @param versionId if versionId = None uses latest
       * @return with metadata (as WrappedSchema). Error if version Id does not exist or if versionId is None, there is
       *         no latest
       */
      def version(subject: String, versionId: Option[Int]): RestResponse[WrappedSchema] =
      wrappedSchemaResponse(versionId.fold {
          jrs.getLatestVersion(subject)
        }
        {
          version => jrs.getVersion(subject, version)
        })


      /**
       * registers a schema with given subject, returning unique schema id in registry
       * @param subject
       * @param schema
       * @return unique schema id
       */
      def registerSchema(subject: String, schema: avro.Schema): RestResponse[Int] = for {
        result <- ZIO.effect(jrs.registerSchema(schema.toString, subject))
      } yield result

      /**
       * checks if schema already defined for given subject. If so returns schema
       * @param subject
       * @param schema
       * @return schema with metadata as WrappedSchema
       */
      def alreadyPresent(subject: String, schema: avro.Schema): RestResponse[Option[WrappedSchema]] =
        try {
          val cs = jrs.lookUpSubjectVersion(schema.toString, subject)
          parseWrapped(cs).map(Some(_))
        } catch {
          case e: RestClientException =>
            if (e.getErrorCode == 40403)
              IO.effectTotal(Option.empty[WrappedSchema])
            else failSchemaError[Option[WrappedSchema]](e)
          case x: Throwable => IO.fail(x)
        }

      /**
       * deletes a specific version of a schema
       * @param subject
       * @param versionId
       * @return Unit - error if does not exist
       */
      def delete(subject: String, versionId: Int): RestResponse[Unit] =
      restResponse{
        jrs.deleteSchemaVersion(Map.empty[String, String].asJava, subject, versionId.toString)
        ()
      }


      /**
       * Checks compatibility of proposed schema with the schema identified by subject / versionId according to the current
       * compatibilityLevel
       * @param subject
       * @param versionId
       * @param schema
       * @return true if compatible else false
       */
      def compatible(subject: String, versionId: Int, schema: avro.Schema): RestResponse[Boolean] =
      restResponse(jrs.testCompatibility(schema.toString, subject, versionId.toString))


      /**
       * Sets compatibility level for registry server as a whole
       * @param compatibilityLevel new compatibility level
       * @return Unit
       */
      def setConfig(compatibilityLevel: CompatibilityLevel): RestResponse[Unit] =
      restResponse {
          jrs.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), null)
          ()
        } // yuk!


      /**
       * gets current compatility level for registry server as a whole
       * @return CompatibilityLevel
       */
      def config: RestResponse[CompatibilityLevel] =
      restResponse {
          val config = jrs.getConfig(null)
          val level = config.getCompatibilityLevel
          CompatibilityLevel.values.find(_._2 == level).get._1
        }

      /**
       * sets the compatibilityLevel for a speciric subject
       * @param subject subject name
       * @param compatibilityLevel desired level
       * @return Unit
       */
      def setConfig(subject: String, compatibilityLevel: CompatibilityLevel): RestResponse[Unit] = for {
        result <- ZIO.effect {
          jrs.updateCompatibility(CompatibilityLevel.values(compatibilityLevel), subject)
          ()
        }
      } yield result


      /**
       * gets compatibilityLevel for given subject
       * @param subject
       * @return the level for the subject. (Confluent docs do not define behaviour if none set for this subject but presumably
       *         defaults to server compatibility level)
       */
      def config(subject: String): RestResponse[CompatibilityLevel] = for {
        result <- ZIO.succeed {
          val config = jrs.getConfig(subject)
          val level = config.getCompatibilityLevel
          CompatibilityLevel.values.find(_._2 == level).get._1
        }
      } yield result

      /**
       * Creates a serializer which serializes case classes [T] to a generic record.
       * It performs a registry lookup and uses schema ids in the output data rather than
       * embed the whole schema.
       *
       * @param subjectNameStrategy a SubjectNameStrategy
       * @param additionalParams any additional parameters as required.  You do not need to supply registry url
       * @return the AvroSerializer object
       */
      def avroSerializer(subjectNameStrategy: SubjectNameStrategy,
                        additionalParams: Map[String, Any] = Map.empty): ZIO[Any,Throwable,AvroSerializer] = for {
        result <- b.effectBlocking {
          val params = (additionalParams ++
            List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
          AvroSerializer(new KafkaAvroSerializer(registryClient, params))
        }
      } yield result

      /**
       * Creates a deserializer which will work with a given T.
       * It will fail if the data is not of the correct type.
       * It only supports those type supported by the registry.
       * The deserializer uses the schema id in the bytes to perform a registry
       * lookup which it can then use to deserialize into a generic record.
       *
       *
       * @param subjectNameStrategy
       * @param additionalParams any additional parameters as required. You do not need to supply registry url
       * @tparam T
       * @return
       */
      def avroDeserializer[T : ClassTag](subjectNameStrategy: SubjectNameStrategy,
                          additionalParams: Map[String, Any] = Map.empty): ZIO[Any,Throwable,AvroDeserializer[T]] = for {
        result <- b.effectBlocking {
          val params = (additionalParams ++
            List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
              AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
          AvroDeserializer[T](new KafkaAvroDeserializer(registryClient, params))
        }
      } yield result

      /**
       * creates a deserializer that expects a generic record and converts to type T using the supplied avro4s recordFormat
       * @param subjectNameStrategy
       * @param additionalParams any additional parameters as required. You do not need to supply registry url
       * @param recordFormat
       * @tparam T the target type
       * @return a serializer for the specific type
       */
      def avroGenericDeserializer[T](subjectNameStrategy: SubjectNameStrategy,
                                    additionalParams: Map[String, Any] = Map.empty)
                                    (recordFormat: RecordFormat[T]): ZIO[Any,Throwable,AvroGenericDeserializer[T]] = for {
        result <- b.effectBlocking {
          val params = (additionalParams ++
            List(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY -> subjectNameStrategy.strategyClassName,
              AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> url)).asJava
          AvroGenericDeserializer[T](new KafkaAvroDeserializer(registryClient, params), recordFormat)
        }
      } yield result


    }}
    // Accessor methods
    def subjects = ZIO.accessM[ConfluentClientService](_.get.subjects)
    /**
       * registers a schema with given subject, returning unique schema id in registry
       * @param subject
       * @param schema
       * @return unique schema id
       */
    def registerSchema(subject: String, schema: AvroSchema) = ZIO.accessM[ConfluentClientService](_.get.registerSchema(subject, schema))
    def alreadyPresent(subject: String, schema: AvroSchema) = ZIO.accessM[ConfluentClientService](_.get.alreadyPresent(subject, schema))
    def schema(id: Int) = ZIO.accessM[ConfluentClientService](_.get.schema(id))
    def delete(subject: String, versionId: Int) = ZIO.accessM[ConfluentClientService](_.get.delete(subject, versionId))
    def deleteSubject(subject: String) = ZIO.accessM[ConfluentClientService](_.get.deleteSubject(subject))
    def avroGenericDeserializer[T](subjectNameStrategy: SubjectNameStrategy,
                                    additionalParams: Map[String, Any] = Map.empty)
                                    (recordFormat: RecordFormat[T]) = ZIO.accessM[ConfluentClientService](_.get.avroGenericDeserializer(subjectNameStrategy, additionalParams)(recordFormat))
    def subjectVersions(subject: String) = ZIO.accessM[ConfluentClientService](_.get.subjectVersions(subject))
    def avroDeserializer[T : ClassTag](subjectNameStrategy: SubjectNameStrategy,
                          additionalParams: Map[String, Any] = Map.empty) = ZIO.accessM[ConfluentClientService](_.get.avroDeserializer(subjectNameStrategy, additionalParams))
    def version(subject: String, versionId: Option[Int]) = ZIO.accessM[ConfluentClientService](_.get.version(subject, versionId))
    def setConfig(subject: String, compatibilityLevel: CompatibilityLevel) = ZIO.accessM[ConfluentClientService](_.get.setConfig(subject, compatibilityLevel))
    def setConfig(compatibilityLevel: CompatibilityLevel) = ZIO.accessM[ConfluentClientService](_.get.setConfig(compatibilityLevel))
    def compatible(subject: String, versionId: Int, schema: avro.Schema) = ZIO.accessM[ConfluentClientService](_.get.compatible(subject, versionId, schema))
    def config = ZIO.accessM[ConfluentClientService](_.get.config)
    def config(subject: String) = ZIO.accessM[ConfluentClientService](_.get.config(subject))
  }
}
