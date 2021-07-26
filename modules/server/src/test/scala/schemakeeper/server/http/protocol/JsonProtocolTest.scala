package schemakeeper.server.http.protocol

import munit._
import schemakeeper.api._
import io.circe.syntax._
import JsonProtocol._
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.http.internal.SubjectSettings

class JsonProtocolTest extends FunSuite {
  test("SubjectSettings should be encoded and decoded correctly") {
    val meta = SubjectSettings(CompatibilityType.BACKWARD, isLocked = true)
    val json = meta.asJson

    assert(json.as[SubjectSettings].contains(meta))
  }

  test("SchemaMetadata should be encoded and decoded correctly") {
    val meta = SchemaMetadata.instance(1, "b", "c")
    val json = meta.asJson

    assert(json.as[SchemaMetadata].contains(meta))
  }

  test("SubjectSchemaMetadata should be encoded and decoded correctly") {
    val meta = SubjectSchemaMetadata.instance(1, 2, "b", "c")
    val json = meta.asJson

    assert(json.as[SubjectSchemaMetadata].contains(meta))
  }

  test("CompatibilityType should be encoded and decoded correctly") {
    val meta = CompatibilityType.BACKWARD
    val json = meta.asJson

    assert(json.as[CompatibilityType].contains(meta))
  }

  test("CompatibilityType should return error because of not existing compatibility type") {
    val json = """{ "compatibilityType": "unknown" }""".asJson

    assert(json.as[CompatibilityType].isLeft)
  }

  test("SubjectMetadata should be encoded and decoded correctly") {
    val meta = SubjectMetadata.instance("a", CompatibilityType.BACKWARD)
    val json = meta.asJson

    assert(json.as[SubjectMetadata].contains(meta))
  }

  test("SubjectMetadata should return error because of not existing compatibility type") {
    val json = """{ "subject": "a", "compatibilityType": "unknown", "format": "b"}""".asJson

    assert(json.as[SubjectMetadata].isLeft)
  }

  test("SchemaText should be encoded and decoded correctly") {
    val meta = SchemaText.instance("schema")
    val json = meta.asJson

    assert(json.as[SchemaText].contains(meta))
  }

  test("SubjectAndSchemaRequest should be encoded and decoded correctly") {
    val meta = SubjectAndSchemaRequest.instance("schema", SchemaType.AVRO, CompatibilityType.NONE)
    val json = meta.asJson

    assert(json.as[SubjectAndSchemaRequest].contains(meta))
  }

  test("SchemaId should be encoded and decoded correctly") {
    val meta = SchemaId.instance(1)
    val json = meta.asJson

    assert(json.as[SchemaId].contains(meta))
  }
}
