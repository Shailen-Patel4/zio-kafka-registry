package zio.kafka.registry
import java.util

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider
import javax.net.ssl.SSLSocketFactory
import org.apache.avro.Schema
import zio.{Semaphore, RIO, ZIO}
import zio.blocking._


/**
 * Wrapper for RestService config (security) settings. See relevant docs for
 * io.confluent.kafka.schemaregistry.client.rest.RestService (if you can find any!)
 *
 * In order to set security you should create a ConfluentClientService and then call the required method
 * on the ConfluentClientService#confluentRestService object
 */
package object confluentRestService {

import zio.kafka.registry.confluentRestServiceReq.ConfluentRestServiceReq

import zio.ZLayer

  import zio.Has
  type ConfluentRestService = Has[ConfluentRestService.Service]
  object ConfluentRestService {
    trait Service {
      private[registry] val jrs: RestService

      /**
       * for use with ssl
       * @param sslSocketFactory
       * @return
       */
      def setSslSocketFactory(sslSocketFactory: SSLSocketFactory): ZIO[Any, Nothing, RestConfigResponse[Unit]]

      /** for use with basic auth */
      def setBasicAuthCredentialProvider(basicAuthCredentialProvider: BasicAuthCredentialProvider) : ZIO[Any, Nothing, RestConfigResponse[Unit]]

      /** for use with bearer auth */
      def setBearerAuthCredentialProvider(bearerAuthCredentialProvider: BearerAuthCredentialProvider) : ZIO[Any, Nothing, RestConfigResponse[Unit]]

      /** http headers to go to schema registry server  */
      def setHttpHeaders(httpHeaders: util.Map[String, String]) : ZIO[Any, Nothing, RestConfigResponse[Unit]]
    }

    case class SchemaError(errorCode: Int, message: String) extends Throwable

    case class WrappedSchema(subject: String, id: Int, version: Int, schema: Schema)


    sealed trait CompatibilityLevel
    case object Backward extends CompatibilityLevel
    case object BackwardTransitive extends CompatibilityLevel
    case object Forward extends CompatibilityLevel
    case object ForwardTransitive extends CompatibilityLevel
    case object Full extends CompatibilityLevel
    case object FullTransitive extends CompatibilityLevel
    case object NoCompatibilityLevel extends CompatibilityLevel // didn't want to muddy the waters with "None"

    object CompatibilityLevel {
      val values: Map[CompatibilityLevel, String] = Map(
        Backward  -> "BACKWARD",
        BackwardTransitive  -> "BACKWARD_TRANSITIVE",
        Forward  -> "FORWARD",
        ForwardTransitive  -> "FORWARD_TRANSITIVE",
        Full  -> "FULL",
        FullTransitive  -> "FULL_TRANSITIVE",
        NoCompatibilityLevel  -> "NONE",
      )
    }
    type RestConfigResponse[T] = RIO[Blocking, T]

    def live(url: String): ZLayer[ConfluentRestServiceReq, Nothing, ConfluentRestService] = ZLayer.fromService {d =>
      new Service {
        /**
         * for use with ssl
         * @param sslSocketFactory
         * @return
         */

        override val jrs: RestService = new RestService(url)

        def setSslSocketFactory(sslSocketFactory: SSLSocketFactory): ZIO[Any,Nothing,RestConfigResponse[Unit]] = for {
          sem <- d.sem
        } yield sem.withPermit(
            effectBlocking(jrs.setSslSocketFactory(sslSocketFactory))
          )

        /** for use with basic auth */
        def setBasicAuthCredentialProvider(basicAuthCredentialProvider: BasicAuthCredentialProvider) : ZIO[Any,Nothing,RestConfigResponse[Unit]] = for {
          sem <- d.sem
        } yield sem.withPermit(
            effectBlocking(jrs.setBasicAuthCredentialProvider(basicAuthCredentialProvider)))

        /** for use with bearer auth */
        def setBearerAuthCredentialProvider(bearerAuthCredentialProvider: BearerAuthCredentialProvider) : ZIO[Any,Nothing,RestConfigResponse[Unit]] = for {
          sem <- d.sem
        } yield sem.withPermit(
            effectBlocking(jrs.setBearerAuthCredentialProvider(bearerAuthCredentialProvider)))

        /** http headers to go to schema registry server  */
        def setHttpHeaders(httpHeaders: util.Map[String, String]) : ZIO[Any,Nothing,RestConfigResponse[Unit]] = for {
          sem <- d.sem
        } yield sem.withPermit(
            effectBlocking(jrs.setHttpHeaders(httpHeaders)))
      }
    }

    def setSslSocketFactory(sslSocketFactory: SSLSocketFactory): ZIO[ConfluentRestService,Nothing,RestConfigResponse[Unit]]= ZIO.accessM(_.get.setSslSocketFactory(sslSocketFactory))
    def setBasicAuthCredentialProvider(basicAuthCredentialProvider: BasicAuthCredentialProvider) : ZIO[ConfluentRestService,Nothing,RestConfigResponse[Unit]] = ZIO.accessM(_.get.setBasicAuthCredentialProvider(basicAuthCredentialProvider))
    def setBearerAuthCredentialProvider(bearerAuthCredentialProvider: BearerAuthCredentialProvider) : ZIO[ConfluentRestService,Nothing,RestConfigResponse[Unit]] = ZIO.accessM(_.get.setBearerAuthCredentialProvider(bearerAuthCredentialProvider))
    def setHttpHeaders(httpHeaders: util.Map[String, String]) : ZIO[ConfluentRestService,Nothing,RestConfigResponse[Unit]] = ZIO.accessM(_.get.setHttpHeaders(httpHeaders))
  }
}