package schemakeeper.server.api.protocol

import org.scalatest.{Matchers, WordSpec}
import schemakeeper.api._
import io.circe.syntax._
import JsonProtocol._
import schemakeeper.schema.CompatibilityType

class JsonProtocolTest extends WordSpec with Matchers {
  "SchemaMetadata" should {
    "be encoded and decoded correctly" in {
      val meta = SchemaMetadata.instance("a", 1, 1, "b")
      val json = meta.asJson

      assert(json.as[SchemaMetadata].contains(meta))
    }
  }

  "CompatibilityTypeMetadata" should {
    "be encoded and decoded correctly" in {
      val meta = CompatibilityTypeMetadata.instance(CompatibilityType.BACKWARD)
      val json = meta.asJson

      assert(json.as[CompatibilityTypeMetadata].contains(meta))
    }

    "return error because of not existing compatibility type" in {
      val json = """{ "compatibilityType": "unknown" }""".asJson

      assert(json.as[CompatibilityTypeMetadata].isLeft)
    }
  }

  "SubjectMetadata" should {
    "be encoded and decoded correctly" in {
      val meta = SubjectMetadata.instance("a", CompatibilityType.BACKWARD, "b")
      val json = meta.asJson

      assert(json.as[SubjectMetadata].contains(meta))
    }

    "return error because of not existing compatibility type" in {
      val json = """{ "subject": "a", "compatibilityType": "unknown", "format": "b"}""".asJson

      assert(json.as[SubjectMetadata].isLeft)
    }
  }

  "SchemaResponse" should {
    "be encoded and decoded correctly" in {
      val meta = SchemaResponse.instance("schema")
      val json = meta.asJson

      assert(json.as[SchemaResponse].contains(meta))
    }
  }

  "SchemaRequest" should {
    "be encoded and decoded correctly" in {
      val meta = SchemaRequest.instance("schema")
      val json = meta.asJson

      assert(json.as[SchemaRequest].contains(meta))
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
