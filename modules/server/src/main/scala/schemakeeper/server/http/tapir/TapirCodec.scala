package schemakeeper.server.http.tapir

import java.nio.charset.StandardCharsets

import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import io.circe.parser.{decode => cDecode}
import sttp.tapir.CodecFormat.Json
import sttp.tapir.{Codec, CodecFormat, CodecMeta, DecodeResult, Schema, SchemaType => TapirSchemaType, StringValueType}
import schemakeeper.api._
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.http.internal.SubjectSettings
import schemakeeper.server.http.protocol.ErrorInfo

object TapirCodec {
  import schemakeeper.server.http.protocol.JsonProtocol._

  def codec[A: Encoder: Decoder: Schema]: Codec[A, Json, String] = new Codec[A, Json, String] {
    override def encode(t: A): String = t.asJson.noSpacesSortKeys

    override def rawDecode(s: String): DecodeResult[A] = cDecode[A](s) match {
      case Left(f)  => DecodeResult.Error(s, f)
      case Right(v) => DecodeResult.Value(v)
    }

    override def meta: CodecMeta[A, Json, String] =
      CodecMeta(implicitly, CodecFormat.Json(), StringValueType(StandardCharsets.UTF_8))
  }

  implicit def schemaForCustomType[A]: Schema[A] = Schema(TapirSchemaType.SString)

  implicit val errorInfoCodec = codec[ErrorInfo]
  implicit val schemaMetadataCodec = codec[SchemaMetadata]
  implicit val subjectSettingsCodec = codec[SubjectSettings]
  implicit val subjectSchemaMetadataCodec = codec[SubjectSchemaMetadata]
  implicit val listOfSubjectSchemaMetadataCodec = codec[List[SubjectSchemaMetadata]]
  implicit val compatibilityTypeCodec = codec[CompatibilityType]
  implicit val subjectMetadataCodec = codec[SubjectMetadata]
  implicit val schemaTextCodec = codec[SchemaText]
  implicit val schemaIdCodec = codec[SchemaId]
  implicit val subjectAndSchemaRequestCodec = codec[SubjectAndSchemaRequest]
  implicit val schemaTypeCodec = codec[SchemaType]
}
