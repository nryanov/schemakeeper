package schemakeeper.server.api

import org.slf4j.LoggerFactory
import cats.effect.Sync
import schemakeeper.api._
import schemakeeper.schema.CompatibilityType
import schemakeeper.server.api.internal.SubjectSettings
import schemakeeper.server.api.protocol.{ErrorCode, ErrorInfo}
import schemakeeper.server.service._
import schemakeeper.server.api.protocol.JsonProtocol._
import schemakeeper.server.SchemaKeeperError._
import sttp.tapir._
import sttp.tapir.json.circe._
import sttp.tapir.server.http4s._
import io.circe.generic.auto._
import schemakeeper.server.SchemaKeeperError
import sttp.model.StatusCode

class SchemaKeeperApi[F[_]: Sync](storage: Service[F]) {

  import SchemaKeeperApi._

  private val baseEndpoint: Endpoint[Unit, (StatusCode, ErrorInfo), Unit, Nothing] =
    endpoint.in(apiVersion).errorOut(statusCode.and(jsonBody[ErrorInfo]))

  private val subjectsEndpoint: Endpoint[Unit, (StatusCode, ErrorInfo), List[String], Nothing] =
    baseEndpoint.get.in("subjects").out(jsonBody[List[String]])

  private val subjectMetadataEndpoint: Endpoint[String, (StatusCode, ErrorInfo), SubjectMetadata, Nothing] =
    baseEndpoint.get.in("subjects").in(path[String]).out(jsonBody[SubjectMetadata])

  private val updateSubjectSettingsEndpoint
    : Endpoint[(String, SubjectSettings), (StatusCode, ErrorInfo), SubjectMetadata, Nothing] =
    baseEndpoint.put.in("subjects").in(path[String]).in(jsonBody[SubjectSettings]).out(jsonBody[SubjectMetadata])

  private val subjectVersionsEndpoint: Endpoint[String, (StatusCode, ErrorInfo), List[Int], Nothing] =
    baseEndpoint.get.in("subjects").in(path[String]).in("versions").out(jsonBody[List[Int]])

  private val subjectSchemasMetadataEndpoint
    : Endpoint[String, (StatusCode, ErrorInfo), List[SubjectSchemaMetadata], Nothing] =
    baseEndpoint.get.in("subjects").in(path[String]).in("schemas").out(jsonBody[List[SubjectSchemaMetadata]])

  private val subjectSchemaByVersionEndpoint
    : Endpoint[(String, Int), (StatusCode, ErrorInfo), SubjectSchemaMetadata, Nothing] =
    baseEndpoint.get
      .in("subjects")
      .in(path[String])
      .in("versions")
      .in(path[Int].validate(Validator.min(1)))
      .out(jsonBody[SubjectSchemaMetadata])

  private val schemaByIdEndpoint: Endpoint[Int, (StatusCode, ErrorInfo), SchemaMetadata, Nothing] =
    baseEndpoint.get.in("schemas").in(path[Int].validate(Validator.min(1))).out(jsonBody[SchemaMetadata])

  private val schemaIdBySubjectAndSchemaEndpoint
    : Endpoint[(String, SchemaText), (StatusCode, ErrorInfo), SchemaId, Nothing] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("schemas" / "id")
      .in(jsonBody[SchemaText])
      .out(jsonBody[SchemaId])

  private val deleteSubjectEndpoint: Endpoint[String, (StatusCode, ErrorInfo), Boolean, Nothing] =
    baseEndpoint.delete.in("subjects").in(path[String]).out(jsonBody[Boolean])

  private val deleteSubjectSchemaByVersionEndpoint: Endpoint[(String, Int), (StatusCode, ErrorInfo), Boolean, Nothing] =
    baseEndpoint.delete
      .in("subjects")
      .in(path[String])
      .in("version")
      .in(path[Int].validate(Validator.min(1)))
      .out(jsonBody[Boolean])

  private val checkSubjectSchemaCompatibilityEndpoint
    : Endpoint[(String, SchemaText), (StatusCode, ErrorInfo), Boolean, Nothing] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("compatibility" / "schemas")
      .in(jsonBody[SchemaText])
      .out(jsonBody[Boolean])

  private val registerSchemaEndpoint: Endpoint[SchemaText, (StatusCode, ErrorInfo), SchemaId, Nothing] =
    baseEndpoint.post.in("schemas").in(jsonBody[SchemaText]).out(jsonBody[SchemaId])

  //          case _ @SubjectIsAlreadyConnectedToSchema(_, schemaId) => Ok(SchemaId.instance(schemaId))
  private val registerSchemaAndSubjectEndpoint
    : Endpoint[(String, SubjectAndSchemaRequest), (StatusCode, ErrorInfo), SchemaId, Nothing] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("schemas")
      .in(jsonBody[SubjectAndSchemaRequest])
      .out(jsonBody[SchemaId])

  private val registerSubjectEndpoint: Endpoint[SubjectMetadata, (StatusCode, ErrorInfo), SubjectMetadata, Nothing] =
    baseEndpoint.post.in("subjects").in(jsonBody[SubjectMetadata]).out(jsonBody[SubjectMetadata])

  private val addSchemaToSubjectEndpoint: Endpoint[(String, Int), (StatusCode, ErrorInfo), Int, Nothing] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("schemas")
      .in(path[Int].validate(Validator.min(1)))
      .out(jsonBody[Int])

  private def handleError(err: Throwable): (StatusCode, ErrorInfo) = err match {
    case e: BackendError           => (StatusCode.InternalServerError, ErrorInfo(e.msg, ErrorCode.BackendErrorCode))
    case e: SubjectDoesNotExist    => (StatusCode.NotFound, ErrorInfo(e.msg, ErrorCode.SubjectDoesNotExistCode))
    case e: SubjectIsAlreadyExists => (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SubjectIsAlreadyExistsCode))
    case e: SubjectHasNoRegisteredSchemas =>
      (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SubjectHasNoRegisteredSchemasCode))
    case e: SubjectSchemaVersionDoesNotExist =>
      (StatusCode.NotFound, ErrorInfo(e.msg, ErrorCode.SubjectSchemaVersionDoesNotExistCode))
    case e: SchemaIdDoesNotExist  => (StatusCode.NotFound, ErrorInfo(e.msg, ErrorCode.SchemaIdDoesNotExistCode))
    case e: SchemaIsNotRegistered => (StatusCode.NotFound, ErrorInfo(e.msg, ErrorCode.SchemaIsNotRegisteredCode))
    case e: SchemaIsNotValid      => (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SchemaIsNotValidCode))
    case e: SchemaIsAlreadyExist  => (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SchemaIsAlreadyExistCode))
    case e: SubjectIsAlreadyConnectedToSchema =>
      (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SubjectIsAlreadyConnectedToSchemaCode))
    case e: SubjectIsNotConnectedToSchema =>
      (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SubjectIsNotConnectedToSchemaCode))
    case e: SchemaIsNotCompatible => (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SchemaIsNotCompatibleCode))
    case e: SubjectIsLocked       => (StatusCode.BadRequest, ErrorInfo(e.msg, ErrorCode.SubjectIsLockedErrorCode))
    case _                        => (StatusCode.InternalServerError, ErrorInfo(err.getLocalizedMessage, ErrorCode.BackendErrorCode))
  }
}

object SchemaKeeperApi {
  private val logger = LoggerFactory.getLogger(SchemaKeeperApi.getClass)
  private val apiVersion = "v2"

  def create[F[_]: Sync](service: Service[F]): SchemaKeeperApi[F] = new SchemaKeeperApi(service)
}
