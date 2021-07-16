package schemakeeper.server.http.protocol

import org.scalatest.{Matchers, WordSpec}
import schemakeeper.api._
import io.circe.syntax._
import JsonProtocol._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.http.internal.SubjectSettings

@RunWith(classOf[JUnitRunner])
class JsonProtocolTest extends WordSpec with Matchers {
  "SubjectSettings" should {
    "be encoded and decoded correctly" in {
      val meta = SubjectSettings(CompatibilityType.BACKWARD, isLocked = true)
      val json = meta.asJson

      assert(json.as[SubjectSettings].contains(meta))
    }
  }

  "SchemaMetadata" should {
    "be encoded and decoded correctly" in {
      val meta = SchemaMetadata.instance(1, "b", "c")
      val json = meta.asJson

      assert(json.as[SchemaMetadata].contains(meta))
    }
  }

  "SubjectSchemaMetadata" should {
    "be encoded and decoded correctly" in {
      val meta = SubjectSchemaMetadata.instance(1, 2, "b", "c")
      val json = meta.asJson

      assert(json.as[SubjectSchemaMetadata].contains(meta))
    }
  }

  "CompatibilityType" should {
    "be encoded and decoded correctly" in {
      val meta = CompatibilityType.BACKWARD
      val json = meta.asJson

      assert(json.as[CompatibilityType].contains(meta))
    }

    "return error because of not existing compatibility type" in {
      val json = """{ "compatibilityType": "unknown" }""".asJson

      assert(json.as[CompatibilityType].isLeft)
    }
  }

  "SubjectMetadata" should {
    "be encoded and decoded correctly" in {
      val meta = SubjectMetadata.instance("a", CompatibilityType.BACKWARD)
      val json = meta.asJson

      assert(json.as[SubjectMetadata].contains(meta))
    }

    "return error because of not existing compatibility type" in {
      val json = """{ "subject": "a", "compatibilityType": "unknown", "format": "b"}""".asJson

      assert(json.as[SubjectMetadata].isLeft)
    }
  }

  "SchemaText" should {
    "be encoded and decoded correctly" in {
      val meta = SchemaText.instance("schema")
      val json = meta.asJson

      assert(json.as[SchemaText].contains(meta))
    }
  }

  "SubjectAndSchemaRequest" should {
    "be encoded and decoded correctly" in {
      val meta = SubjectAndSchemaRequest.instance("schema", SchemaType.AVRO, CompatibilityType.NONE)
      val json = meta.asJson

      assert(json.as[SubjectAndSchemaRequest].contains(meta))
    }
  }

  "SchemaId" should {
    "be encoded and decoded correctly" in {
      val meta = SchemaId.instance(1)
      val json = meta.asJson

      assert(json.as[SchemaId].contains(meta))
    }
  }
}
