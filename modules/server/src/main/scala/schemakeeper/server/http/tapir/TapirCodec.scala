package schemakeeper.server.http.tapir

import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import sttp.tapir.CodecFormat.Json
import sttp.tapir.{Codec, DecodeResult, Schema, SchemaType => TapirSchemaType}
import schemakeeper.api._
import schemakeeper.server.http.protocol.JsonProtocol._
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.http.internal.SubjectSettings
import schemakeeper.server.http.protocol.ErrorInfo
import sttp.tapir.json.circe.TapirJsonCirce

object TapirCodec extends TapirJsonCirce {
  def codec[A: Encoder: Decoder: Schema]: Codec[String, A, Json] =
    Codec.json[A](str =>
      str.asJson.as[A] match {
        case Left(error)  => DecodeResult.Error(str, error)
        case Right(value) => DecodeResult.Value(value)
      }
    )(_.asJson.noSpaces)

  implicit def schemaForCustomType[A]: Schema[A] = Schema(TapirSchemaType.SString())

  // codecs for java models
  implicit val errorInfoCodec: Codec[String, ErrorInfo, Json] = codec[ErrorInfo]
  implicit val schemaMetadataCodec: Codec[String, SchemaMetadata, Json] = codec[SchemaMetadata]
  implicit val subjectSettingsCodec: Codec[String, SubjectSettings, Json] = codec[SubjectSettings]
  implicit val subjectSchemaMetadataCodec: Codec[String, SubjectSchemaMetadata, Json] = codec[SubjectSchemaMetadata]
  implicit val listOfSubjectSchemaMetadataCodec: Codec[String, List[SubjectSchemaMetadata], Json] =
    codec[List[SubjectSchemaMetadata]]
  implicit val compatibilityTypeCodec: Codec[String, CompatibilityType, Json] = codec[CompatibilityType]
  implicit val subjectMetadataCodec: Codec[String, SubjectMetadata, Json] = codec[SubjectMetadata]
  implicit val schemaTextCodec: Codec[String, SchemaText, Json] = codec[SchemaText]
  implicit val schemaIdCodec: Codec[String, SchemaId, Json] = codec[SchemaId]
  implicit val subjectAndSchemaRequestCodec: Codec[String, SubjectAndSchemaRequest, Json] =
    codec[SubjectAndSchemaRequest]
  implicit val schemaTypeCodec: Codec[String, SchemaType, Json] = codec[SchemaType]
}
