package zio.kafka.registry

import zio.ZIO
import zio.test._
import Assertion._
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import confluentRestService.ConfluentRestService._
import zio.kafka.registry.confluentClientService.ConfluentClientService

object TestRestSupport {
  case class President1(name: String)
  case class President2(name: String, age: Int)


  val schema1 = AvroSchema[President1]
  val schema2 = AvroSchema[President2]

  val format1 = RecordFormat[President1]
  val format2 = RecordFormat[President2]

    def allTests = List(
     testSubjects,
      testDelete,
      modifyCompatibility,
      checkDeleteSubject,
      multipleSchemas,
      compatibleSchemas
    )

    val testSubjects
    = testM("test subjects empty then non-empty") {
      val subject = "presidents-value"
      for {
        initial <- ConfluentClientService.subjects
        posted <- ConfluentClientService.registerSchema(subject, schema1)
        already <- ConfluentClientService.alreadyPresent(subject, schema1)
        schemaBack <- ConfluentClientService.schema(posted)
        later <- ConfluentClientService.subjects
        _ <- ConfluentClientService.deleteSubject(subject)
      } yield {
        println(initial)
        assert(initial)(not(contains(subject)))
        assert(later)(contains(subject)) &&
        assert(posted)(equalTo(1)) &&
        assert(already)(not(equalTo(None))) &&
        assert(already.get.schema)(equalTo(schema1)) &&
        assert(schemaBack)(equalTo(schema1))
      }
    }

    val testDelete = testM("add then delete") {
      val subject = "morepresidents"
      for {
        initial <- ConfluentClientService.subjects
        _=println(initial)
        posted <- ConfluentClientService.registerSchema(subject, schema1)
        later <- ConfluentClientService.subjects
        _ <- ConfluentClientService.delete(subject, posted)
        laterStill <- ConfluentClientService.subjects
      } yield {
        assert(initial)(not(contains(subject))) &&
          assert(later)(contains(subject)) &&
          assert(posted)(equalTo(1)) &&
          assert(laterStill)(not(contains(subject)))
      }
    }

    val checkDeleteSubject = testM("delete whole subject") {
      val subject = "morepresidents2"
      for {
        posted <- ConfluentClientService.registerSchema(subject, schema1)
        later <- ConfluentClientService.subjects
        _ <- ConfluentClientService.deleteSubject(subject)
        laterStill <- ConfluentClientService.subjects
      } yield {
        assert(later)(contains(subject)) &&
          assert(posted)(equalTo(1)) &&
          assert(laterStill)(not(contains(subject)))
      }
    }

    val multipleSchemas = testM("test with multiple schemas in same subject") {
      val subject = "presidents3"
      for {
        _ <- ConfluentClientService.registerSchema(subject, schema1)
        _ <- ConfluentClientService.setConfig(subject, Forward)
        _ <- ConfluentClientService.registerSchema(subject, schema2)
        versions <- ConfluentClientService.subjectVersions(subject)
      } yield {
        assert(versions)(equalTo(List(1, 2)))
      }
    }

    val compatibleSchemas = testM("schema1 and 2 are compatible") {
      val subject = "presidents4"
      for {
        _ <- ConfluentClientService.registerSchema(subject, schema1)
        compat <- ConfluentClientService.compatible(subject, 1, schema2)
      } yield {
        assertCompletes
      }
    }

    def checkAll(testResults: Iterable[TestResult]) =
      testResults.tail.foldLeft(testResults.head)((acc, it) => acc && it)

    val modifyCompatibility = testM("modify compatibility and check") {

      val subject = "presidentsModifyCompatibility"

      def setCheck(compat: CompatibilityLevel) =
        for {
          x <- ConfluentClientService.setConfig(compat)
          check <- ConfluentClientService.config
        } yield assert(check)(equalTo(compat))

      def setCheck2(compat: CompatibilityLevel) =
        for {
          _ <- ConfluentClientService.setConfig(subject, compat)
          check <- ConfluentClientService.config(subject)
        } yield assert(check)(equalTo(compat))

      for {
        general <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck(compat) })
        bySubject <- ZIO.collectAll(CompatibilityLevel.values.keySet.map { compat => setCheck2(compat) })
      } yield {
        checkAll(general) && checkAll(bySubject)
      }

    }



}

