package schemakeeper.server.api.protocol

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import schemakeeper.api._
import schemakeeper.schema.{CompatibilityType, SchemaType}

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
      ("compatibilityType", Json.fromString(a.getCompatibilityType.identifier))
    )
  }
  implicit val compatibilityTypeMetadataDecoder: Decoder[CompatibilityTypeMetadata] = new Decoder[CompatibilityTypeMetadata] {
    override def apply(c: HCursor): Result[CompatibilityTypeMetadata] = for {
      name <- c.downField("compatibilityType").as[String]
    } yield CompatibilityTypeMetadata.instance(CompatibilityType.findByName(name))
  }

  implicit val subjectMetadataEncoder: Encoder[SubjectMetadata] = new Encoder[SubjectMetadata] {
    override def apply(a: SubjectMetadata): Json = Json.obj(
      ("subject", Json.fromString(a.getSubject)),
      ("compatibilityType", Json.fromString(a.getCompatibilityType.identifier)),
      ("schemaType", Json.fromString(a.getSchemaType.identifier)),
      ("versions", Json.fromValues(a.getVersions.map(Json.fromInt))),
    )
  }
  implicit val subjectMetadataDecoder: Decoder[SubjectMetadata] = new Decoder[SubjectMetadata] {
    override def apply(c: HCursor): Result[SubjectMetadata] = for {
      subject <- c.downField("subject").as[String]
      compatibilityType <- c.downField("compatibilityType").as[String]
      schemaType <- c.downField("schemaType").as[String]
      versions <- c.downField("versions").as[Iterable[Int]]
    } yield SubjectMetadata.instance(subject, CompatibilityType.findByName(compatibilityType), SchemaType.findByName(schemaType), versions.toArray)
  }

  implicit val schemaTextEncoder: Encoder[SchemaText] = new Encoder[SchemaText] {
    override def apply(a: SchemaText): Json = Json.obj(
        ("schema", Json.fromString(a.getSchemaText)),
        ("schemaType", Json.fromString(a.getSchemaType.identifier))
    )
  }
  implicit val schemaTextDecoder: Decoder[SchemaText] = new Decoder[SchemaText] {
    override def apply(c: HCursor): Result[SchemaText] = for {
      schema <- c.downField("schema").as[String]
      schemaType <- c.downField("schemaType").as[String]
    } yield SchemaText.instance(schema, SchemaType.findByName(schemaType))
  }

  implicit val schemaIdEncoder: Encoder[SchemaId] = new Encoder[SchemaId] {
    override def apply(a: SchemaId): Json = Json.obj(
      ("id", Json.fromInt(a.getId))
    )
  }
  implicit val schemaIdDecoder: Decoder[SchemaId] = new Decoder[SchemaId] {
    override def apply(c: HCursor): Result[SchemaId] = for {
      id <- c.downField("id").as[Int]
    } yield SchemaId.instance(id)
  }

  implicit val newSubjectRequestEncoder: Encoder[NewSubjectRequest] = new Encoder[NewSubjectRequest] {
    override def apply(a: NewSubjectRequest): Json = Json.obj(
      ("schemaType", Json.fromString(a.getSchemaType.identifier)),
      ("compatibilityType", Json.fromString(a.getCompatibilityType.identifier)),
      ("schema", Json.fromString(a.getSchemaText))
    )
  }
  implicit val newSubjectRequestDecoder: Decoder[NewSubjectRequest] = new Decoder[NewSubjectRequest] {
    override def apply(c: HCursor): Result[NewSubjectRequest] = for {
      schemaType <- c.downField("schemaType").as[String]
      compatibilityType <- c.downField("compatibilityType").as[String]
      schemaText <- c.downField("schema").as[String]
    } yield NewSubjectRequest.instance(schemaText, SchemaType.findByName(schemaType), CompatibilityType.findByName(compatibilityType))
  }
}
