package schemakeeper.server.http.protocol

import io.circe._
import cats.syntax.either._
import schemakeeper.api._
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.http.internal.SubjectSettings

object JsonProtocol {
  implicit val errorInfoEncoder: Encoder[ErrorInfo] = (a: ErrorInfo) =>
    Json.obj(
      ("reason", Json.fromString(a.reason)),
      ("code", Json.fromInt(a.code.code))
    )

  implicit val errorInfoDecoder: Decoder[ErrorInfo] = (c: HCursor) =>
    for {
      reason <- c.downField("reason").as[String]
      code <- c.downField("code").as[Int]
    } yield ErrorInfo(reason, ErrorCode.fromCode(code))

  implicit val schemaMetadataEncoder: Encoder[SchemaMetadata] = (a: SchemaMetadata) =>
    Json.obj(
      ("schemaId", Json.fromInt(a.getSchemaId)),
      ("schemaText", Json.fromString(a.getSchemaText)),
      ("schemaHash", Json.fromString(a.getSchemaHash)),
      ("schemaType", Json.fromString(a.getSchemaType.identifier))
    )

  implicit val schemaMetadataDecoder: Decoder[SchemaMetadata] = (c: HCursor) =>
    for {
      schemaId <- c.downField("schemaId").as[Int]
      schemaText <- c.downField("schemaText").as[String]
      schemaHash <- c.downField("schemaHash").as[String]
      schemaType <- c.downField("schemaType").as[String]
    } yield SchemaMetadata.instance(schemaId, schemaText, schemaHash, SchemaType.findByName(schemaType))

  implicit val subjectSettingsEncoder: Encoder[SubjectSettings] = (a: SubjectSettings) =>
    Json.obj(
      ("compatibilityType", Json.fromString(a.compatibilityType.identifier)),
      ("isLocked", Json.fromBoolean(a.isLocked))
    )

  implicit val subjectSettingsDecoder: Decoder[SubjectSettings] = (c: HCursor) =>
    for {
      compatibilityType <- c.downField("compatibilityType").as[String]
      isLocked <- c.downField("isLocked").as[Boolean]
    } yield SubjectSettings(CompatibilityType.findByName(compatibilityType), isLocked)

  implicit val subjectSchemaMetadataEncoder: Encoder[SubjectSchemaMetadata] = (a: SubjectSchemaMetadata) =>
    Json.obj(
      ("schemaId", Json.fromInt(a.getSchemaId)),
      ("version", Json.fromInt(a.getVersion)),
      ("schemaText", Json.fromString(a.getSchemaText)),
      ("schemaHash", Json.fromString(a.getSchemaHash)),
      ("schemaType", Json.fromString(a.getSchemaType.identifier))
    )
  implicit val subjectSchemaMetadataDecoder: Decoder[SubjectSchemaMetadata] = (c: HCursor) =>
    for {
      schemaId <- c.downField("schemaId").as[Int]
      version <- c.downField("version").as[Int]
      schemaText <- c.downField("schemaText").as[String]
      schemaHash <- c.downField("schemaHash").as[String]
      schemaType <- c.downField("schemaType").as[String]
    } yield SubjectSchemaMetadata.instance(schemaId, version, schemaText, schemaHash, SchemaType.findByName(schemaType))

  implicit val compatibilityTypeEncoder: Encoder[CompatibilityType] = (a: CompatibilityType) =>
    Json.obj(
      ("compatibilityType", Json.fromString(a.identifier))
    )
  implicit val compatibilityTypeDecoder: Decoder[CompatibilityType] = (c: HCursor) =>
    for {
      name <- c.downField("compatibilityType").as[String]
    } yield CompatibilityType.findByName(name)

  implicit val subjectMetadataEncoder: Encoder[SubjectMetadata] = (a: SubjectMetadata) =>
    Json.obj(
      ("subject", Json.fromString(a.getSubject)),
      ("compatibilityType", Json.fromString(a.getCompatibilityType.identifier)),
      ("isLocked", Json.fromBoolean(a.isLocked))
    )
  implicit val subjectMetadataDecoder: Decoder[SubjectMetadata] = (c: HCursor) =>
    for {
      subject <- c.downField("subject").as[String]
      compatibilityType <- c.downField("compatibilityType").as[String]
      isLocked <- c.downField("isLocked").as[Boolean]
    } yield SubjectMetadata.instance(subject, CompatibilityType.findByName(compatibilityType), isLocked)

  implicit val schemaTextEncoder: Encoder[SchemaText] = (a: SchemaText) =>
    Json.obj(
      ("schemaText", Json.fromString(a.getSchemaText)),
      ("schemaType", Json.fromString(a.getSchemaType.identifier))
    )
  implicit val schemaTextDecoder: Decoder[SchemaText] = (c: HCursor) =>
    for {
      schema <- c.downField("schemaText").as[String]
      schemaType <- c.downField("schemaType").as[String]
    } yield SchemaText.instance(schema, SchemaType.findByName(schemaType))

  implicit val schemaIdEncoder: Encoder[SchemaId] = (a: SchemaId) =>
    Json.obj(
      ("schemaId", Json.fromInt(a.getSchemaId))
    )
  implicit val schemaIdDecoder: Decoder[SchemaId] = (c: HCursor) =>
    for {
      schemaId <- c.downField("schemaId").as[Int]
    } yield SchemaId.instance(schemaId)

  implicit val newSubjectRequestEncoder: Encoder[SubjectAndSchemaRequest] = (a: SubjectAndSchemaRequest) =>
    Json.obj(
      ("schemaType", Json.fromString(a.getSchemaType.identifier)),
      ("compatibilityType", Json.fromString(a.getCompatibilityType.identifier)),
      ("schemaText", Json.fromString(a.getSchemaText))
    )
  implicit val newSubjectRequestDecoder: Decoder[SubjectAndSchemaRequest] = (c: HCursor) =>
    for {
      schemaType <- c.downField("schemaType").as[String]
      compatibilityType <- c.downField("compatibilityType").as[String]
      schemaText <- c.downField("schemaText").as[String]
    } yield SubjectAndSchemaRequest.instance(
      schemaText,
      SchemaType.findByName(schemaType),
      CompatibilityType.findByName(compatibilityType)
    )

  implicit val schemaTypeEncoder: Encoder[SchemaType] = Encoder.encodeString.contramap[SchemaType](_.identifier)

  implicit val schemaTypeDecoder: Decoder[SchemaType] = Decoder.decodeString.emap { str =>
    SchemaType.findByName(str).asRight
  }
}
