package schemakeeper.server.api.protocol

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import schemakeeper.api._
import schemakeeper.schema.{CompatibilityType, SchemaType}

object JsonProtocol {
    implicit val exceptionEncoder: Encoder[Exception] = Encoder.instance {
          case e: ErrorInfo =>
            Json.obj(
              ("reason", Json.fromString(e.reason)),
              ("code", Json.fromInt(e.code.code))
            )
      case e => Json.fromString(e.getLocalizedMessage)
    }

  implicit val schemaMetadataEncoder: Encoder[SchemaMetadata] = new Encoder[SchemaMetadata] {
    override def apply(a: SchemaMetadata): Json = Json.obj(
      ("schemaId", Json.fromInt(a.getSchemaId)),
      ("schemaText", Json.fromString(a.getSchemaText)),
      ("schemaHash", Json.fromString(a.getSchemaHash)),
      ("schemaType", Json.fromString(a.getSchemaType.identifier)),
    )
  }
  implicit val schemaMetadataDecoder: Decoder[SchemaMetadata] = new Decoder[SchemaMetadata] {
    override def apply(c: HCursor): Result[SchemaMetadata] = for {
      schemaId <- c.downField("schemaId").as[Int]
      schemaText <- c.downField("schemaText").as[String]
      schemaHash <- c.downField("schemaHash").as[String]
      schemaType <- c.downField("schemaType").as[String]
    } yield SchemaMetadata.instance(schemaId, schemaText, schemaHash, SchemaType.findByName(schemaType))
  }

  implicit val subjectSchemaMetadataEncoder: Encoder[SubjectSchemaMetadata] = new Encoder[SubjectSchemaMetadata] {
    override def apply(a: SubjectSchemaMetadata): Json = Json.obj(
      ("schemaId", Json.fromInt(a.getSchemaId)),
      ("version", Json.fromInt(a.getVersion)),
      ("schemaText", Json.fromString(a.getSchemaText)),
      ("schemaHash", Json.fromString(a.getSchemaHash)),
      ("schemaType", Json.fromString(a.getSchemaType.identifier)),
    )
  }
  implicit val subjectSchemaMetadataDecoder: Decoder[SubjectSchemaMetadata] = new Decoder[SubjectSchemaMetadata] {
    override def apply(c: HCursor): Result[SubjectSchemaMetadata] = for {
      schemaId <- c.downField("schemaId").as[Int]
      version <- c.downField("version").as[Int]
      schemaText <- c.downField("schemaText").as[String]
      schemaHash <- c.downField("schemaHash").as[String]
      schemaType <- c.downField("schemaType").as[String]
    } yield SubjectSchemaMetadata.instance(schemaId, version, schemaText, schemaHash, SchemaType.findByName(schemaType))
  }

  implicit val compatibilityTypeEncoder: Encoder[CompatibilityType] = new Encoder[CompatibilityType] {
    override def apply(a: CompatibilityType): Json = Json.obj(
      ("compatibilityType", Json.fromString(a.identifier))
    )
  }
  implicit val compatibilityTypeDecoder: Decoder[CompatibilityType] = new Decoder[CompatibilityType] {
    override def apply(c: HCursor): Result[CompatibilityType] = for {
      name <- c.downField("compatibilityType").as[String]
    } yield CompatibilityType.findByName(name)
  }

  implicit val subjectMetadataEncoder: Encoder[SubjectMetadata] = new Encoder[SubjectMetadata] {
    override def apply(a: SubjectMetadata): Json = Json.obj(
      ("subject", Json.fromString(a.getSubject)),
      ("compatibilityType", Json.fromString(a.getCompatibilityType.identifier)),
    )
  }
  implicit val subjectMetadataDecoder: Decoder[SubjectMetadata] = new Decoder[SubjectMetadata] {
    override def apply(c: HCursor): Result[SubjectMetadata] = for {
      subject <- c.downField("subject").as[String]
      compatibilityType <- c.downField("compatibilityType").as[String]
    } yield SubjectMetadata.instance(subject, CompatibilityType.findByName(compatibilityType))
  }

  implicit val schemaTextEncoder: Encoder[SchemaText] = new Encoder[SchemaText] {
    override def apply(a: SchemaText): Json = Json.obj(
      ("schemaText", Json.fromString(a.getSchemaText)),
      ("schemaType", Json.fromString(a.getSchemaType.identifier))
    )
  }
  implicit val schemaTextDecoder: Decoder[SchemaText] = new Decoder[SchemaText] {
    override def apply(c: HCursor): Result[SchemaText] = for {
      schema <- c.downField("schemaText").as[String]
      schemaType <- c.downField("schemaType").as[String]
    } yield SchemaText.instance(schema, SchemaType.findByName(schemaType))
  }

  implicit val schemaIdEncoder: Encoder[SchemaId] = new Encoder[SchemaId] {
    override def apply(a: SchemaId): Json = Json.obj(
      ("schemaId", Json.fromInt(a.getSchemaId))
    )
  }
  implicit val schemaIdDecoder: Decoder[SchemaId] = new Decoder[SchemaId] {
    override def apply(c: HCursor): Result[SchemaId] = for {
      schemaId <- c.downField("schemaId").as[Int]
    } yield SchemaId.instance(schemaId)
  }

  implicit val newSubjectRequestEncoder: Encoder[SubjectAndSchemaRequest] = new Encoder[SubjectAndSchemaRequest] {
    override def apply(a: SubjectAndSchemaRequest): Json = Json.obj(
      ("schemaType", Json.fromString(a.getSchemaType.identifier)),
      ("compatibilityType", Json.fromString(a.getCompatibilityType.identifier)),
      ("schemaText", Json.fromString(a.getSchemaText))
    )
  }
  implicit val newSubjectRequestDecoder: Decoder[SubjectAndSchemaRequest] = new Decoder[SubjectAndSchemaRequest] {
    override def apply(c: HCursor): Result[SubjectAndSchemaRequest] = for {
      schemaType <- c.downField("schemaType").as[String]
      compatibilityType <- c.downField("compatibilityType").as[String]
      schemaText <- c.downField("schemaText").as[String]
    } yield SubjectAndSchemaRequest.instance(schemaText, SchemaType.findByName(schemaType), CompatibilityType.findByName(compatibilityType))
  }
}
