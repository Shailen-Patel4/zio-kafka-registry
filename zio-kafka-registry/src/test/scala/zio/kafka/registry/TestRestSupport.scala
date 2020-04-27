package zio.kafka.registry

import zio.ZIO
import zio.test._
import zio.test.environment.TestEnvironment
import zio.clock.Clock
import zio.console.Console
import zio.blocking.Blocking
import Assertion._
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import confluentRestService.ConfluentRestService._
import zio.kafka.registry.confluentClientService.ConfluentClientService.subjects
import zio.blocking.Blocking
import zio.kafka.registry.confluentRestService.ConfluentRestService
import zio.kafka.registry.confluentClientService.ConfluentClientService
import zio.kafka.registry.confluentRestServiceReq.ConfluentRestServiceReq

object TestRestSupport {
  case class President1(name: String)
  case class President2(name: String, age: Int)


  val schema1 = AvroSchema[President1]
  val schema2 = AvroSchema[President2]

  val format1 = RecordFormat[President1]
  val format2 = RecordFormat[President2]

    def allTests = List(
     testSubjects,
      // testDelete,
      // modifyCompatibility,
      // checkDeleteSubject,
      // multipleSchemas,
      // compatibleSchemas
    )

    val testSubjects
    = testM("test subjects empty then non-empty") {
      val subject = "presidents-value"
      val crlr = ConfluentRestServiceReq.live
      val crl = crlr >>> ConfluentRestService.live("http://schemaregistry.focaldata.dev")
      val testLayer = (crl ++crlr ++ Blocking.live) >>> ConfluentClientService.live("http://schemaregistry.focaldata.dev", 1000)
      for {
        // restClient <- ZIO.environment[ConfluentClientService].map(_.confluentClient)
        initial <- ConfluentClientService.subjects.provideLayer(testLayer)
        posted <- ConfluentClientService.registerSchema(subject, schema2).provideLayer(testLayer)
        already <- ConfluentClientService.alreadyPresent(subject, schema2).provideLayer(testLayer)
        schemaBack <- ConfluentClientService.schema(posted).provideLayer(testLayer)
        later <- ConfluentClientService.subjects.provideLayer(testLayer)
        // _ <- ConfluentClientService.deleteSubject(subject).provideLayer(testLayer)
      } yield {
        println(initial)
        assert(initial)(not(contains(subject)))
        assert(later)(contains(subject)) &&
        assert(posted)(equalTo(47)) &&
        assert(already)(not(equalTo(None))) &&
        assert(already.get.schema)(equalTo(schema2)) &&
        assert(schemaBack)(equalTo(schema2))
      }
    }

    // val testDelete = testM("add then delete") {
    //   val subject = "morepresidents"
    //   val crlr = ConfluentRestServiceReq.live
    //   val crl = crlr >>> ConfluentRestService.live("http://schemaregistry.focaldata.dev")
    //   val testLayer = (crl ++crlr ++ Blocking.live) >>> ConfluentClientService.live("http://schemaregistry.focaldata.dev", 1000)
    //   for {
    //     // restClient <- ZIO.environment[ConfluentClientService].map(_.confluentClient)
    //     initial <- ConfluentClientService.subjects.provideLayer(testLayer)
    //     posted <- ConfluentClientService.registerSchema(subject, schema1).provideLayer(testLayer)
    //     later <- ConfluentClientService.subjects.provideLayer(testLayer)
    //     _ <- ConfluentClientService.delete(subject, posted).provideLayer(testLayer)
    //     laterStill <- ConfluentClientService.subjects.provideLayer(testLayer)
    //   } yield {
    //     assert(initial)(not(contains(subject))) &&
    //       assert(later)(contains(subject)) &&
    //       assert(posted)(equalTo(1)) &&
    //       assert(laterStill)(not(contains(subject)))
    //   }
    // }

    // val checkDeleteSubject = testM("delete whole subject") {
    //   val subject = "morepresidents2"
    //   val crlr = ConfluentRestServiceReq.live
    //   val crl = crlr >>> ConfluentRestService.live("http://schemaregistry.focaldata.dev")
    //   val testLayer = (crl ++crlr ++ Blocking.live) >>> ConfluentClientService.live("http://schemaregistry.focaldata.dev", 1000)
    //   for {
    //     // restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
    //     posted <- ConfluentClientService.registerSchema(subject, schema1).provideLayer(testLayer)
    //     later <- ConfluentClientService.subjects.provideLayer(testLayer)
    //     _ <- ConfluentClientService.deleteSubject(subject).provideLayer(testLayer)
    //     laterStill <- ConfluentClientService.subjects.provideLayer(testLayer)
    //   } yield {
    //     assert(later, contains(subject)) &&
    //       assert(posted, equalTo(1)) &&
    //       assert(laterStill, not(contains(subject)))
    //   }
    // }

    // val multipleSchemas = testM("test with multiple schemas in same subject") {
    //   val subject = "presidents3"
    //   val crlr = ConfluentRestServiceReq.live
    //   val crl = crlr >>> ConfluentRestService.live("http://schemaregistry.focaldata.dev")
    //   val testLayer = (crl ++crlr ++ Blocking.live) >>> ConfluentClientService.live("http://schemaregistry.focaldata.dev", 1000)
    //   for {
    //     // restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
    //     _ <- ConfluentClientService.registerSchema(subject, schema1).provideLayer(testLayer)
    //     _ <- ConfluentClientService.setConfig(subject, Forward).provideLayer(testLayer)
    //     _ <- ConfluentClientService.registerSchema(subject, schema2).provideLayer(testLayer)
    //     versions <- ConfluentClientService.subjectVersions(subject).provideLayer(testLayer)
    //   } yield {
    //     assert(versions, equalTo(List(1, 2)))
    //   }
    // }

    // val compatibleSchemas = testM("schema1 and 2 are compatible") {
    //   val subject = "presidents4"
    //   for {
    //     restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)
    //     _ <- restClient.registerSchema(subject, schema1)
    //     compat <- restClient.compatible(subject, 1, schema2)
    //   } yield {
    //     assertCompletes
    //   }
    // }

    // def checkAll(testResults: Iterable[TestResult]) =
    //   testResults.tail.foldLeft(testResults.head)((acc, it) => acc && it)

    // val modifyCompatibility = testM("modify compatibility and check") {

    //   val subject = "presidentsModifyCompatibility"

    //   def setCheck(restClient: ConfluentClientService, compat: CompatibilityLevel) =
    //     for {
    //       x <- restClient.setConfig(compat)
    //       check <- restClient.config
    //     } yield assert(check, equalTo(compat))

    //   def setCheck2(restClient: ConfluentClientService, compat: CompatibilityLevel) =
    //     for {
    //       _ <- restClient.setConfig(subject, compat)
    //       check <- restClient.config(subject)
    //     } yield assert(check, equalTo(compat))

    //   for {
    //     restClient <- ZIO.environment[ConfluentClient].map(_.confluentClient)

    //     general <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck(restClient, compat) })
    //     bySubject <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck2(restClient, compat) })
    //   } yield {
    //     checkAll(general) && checkAll(bySubject)
    //   }

    // }



}

