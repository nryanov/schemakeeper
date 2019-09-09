package schemakeeper.server.api.protocol

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import schemakeeper.api.{CompatibilityTypeMetadata, SchemaMetadata, SubjectMetadata}
import schemakeeper.server.util.Utils

object JsonProtocol {
  implicit val schemaMetadataEncoder: Encoder[SchemaMetadata] = new Encoder[SchemaMetadata] {
    override def apply(a: SchemaMetadata): Json = Json.obj(
      ("subject", Json.fromString(a.getSubject)),
      ("id", Json.fromInt(a.getId)),
      ("version", Json.fromInt(a.getVersion)),
      ("schema", Json.fromString(a.getSchemaText)),
    )
  }
  implicit val schemaMetadataDecoder: Decoder[SchemaMetadata] = new Decoder[SchemaMetadata] {
    override def apply(c: HCursor): Result[SchemaMetadata] = for {
      subject <- c.downField("subject").as[String]
      id <- c.downField("id").as[Int]
      version <- c.downField("version").as[Int]
      schemaText <- c.downField("schema").as[String]
    } yield SchemaMetadata.instance(subject, id, version, schemaText)
  }

  implicit val compatibilityTypeMetadataEncoder: Encoder[CompatibilityTypeMetadata] = new Encoder[CompatibilityTypeMetadata] {
    override def apply(a: CompatibilityTypeMetadata): Json = Json.obj(
      ("compatibilityType", Json.fromString(a.getCompatibilityType.name()))
    )
  }
  implicit val compatibilityTypeMetadataDecoder: Decoder[CompatibilityTypeMetadata] = new Decoder[CompatibilityTypeMetadata] {
    override def apply(c: HCursor): Result[CompatibilityTypeMetadata] = for {
      name <- c.downField("compatibilityType").as[String]
    } yield CompatibilityTypeMetadata.instance(Utils.compatibilityTypeFromStringUnsafe(name))
  }

  implicit val subjectMetadataEncoder: Encoder[SubjectMetadata] = new Encoder[SubjectMetadata] {
    override def apply(a: SubjectMetadata): Json = Json.obj(
      ("subject", Json.fromString(a.getSubject)),
      ("compatibilityType", Json.fromString(a.getCompatibilityType.name())),
      ("format", Json.fromString(a.getFormatName)),
    )
  }
  implicit val subjectMetadataDecoder: Decoder[SubjectMetadata] = new Decoder[SubjectMetadata] {
    override def apply(c: HCursor): Result[SubjectMetadata] = for {
      subject <- c.downField("subject").as[String]
      compatibilityType <- c.downField("compatibilityType").as[String]
      format <- c.downField("format").as[String]
    } yield SubjectMetadata.instance(subject, Utils.compatibilityTypeFromStringUnsafe(compatibilityType), format)
  }
}
