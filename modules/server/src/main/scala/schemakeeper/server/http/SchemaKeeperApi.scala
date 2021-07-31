package schemakeeper.server.http

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.functor._
import cats.syntax.either._
import cats.syntax.semigroupk._
import schemakeeper.api._
import schemakeeper.server.http.protocol.{ErrorCode, ErrorInfo}
import schemakeeper.server.service._
import schemakeeper.server.SchemaKeeperError._
import org.http4s.HttpRoutes
import schemakeeper.server.http.internal.SubjectSettings
import schemakeeper.server.http.protocol.JsonProtocol._
import schemakeeper.server.http.tapir.TapirCodec._
import sttp.tapir._
import sttp.tapir.server.http4s._
import sttp.model.StatusCode

class SchemaKeeperApi[F[_]: Timer: Concurrent: ContextShift](storage: Service[F]) {

  import SchemaKeeperApi._

  private val baseEndpoint: Endpoint[Unit, (StatusCode, ErrorInfo), Unit, Any] =
    endpoint.in(apiVersion).errorOut(statusCode.and(jsonBody[ErrorInfo]))

  val subjectsEndpoint: Endpoint[Unit, (StatusCode, ErrorInfo), List[String], Any] =
    baseEndpoint.get.in("subjects").out(jsonBody[List[String]])

  val subjectsRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(subjectsEndpoint)(_ => toRoute(storage.subjects()))

  val subjectMetadataEndpoint: Endpoint[String, (StatusCode, ErrorInfo), SubjectMetadata, Any] =
    baseEndpoint.get.in("subjects").in(path[String]).out(jsonBody[SubjectMetadata])

  val subjectMetadataRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(subjectMetadataEndpoint)(subject => toRoute(storage.subjectMetadata(subject)))

  val updateSubjectSettingsEndpoint
    : Endpoint[(String, SubjectSettings), (StatusCode, ErrorInfo), SubjectMetadata, Any] =
    baseEndpoint.put.in("subjects").in(path[String]).in(jsonBody[SubjectSettings]).out(jsonBody[SubjectMetadata])

  val updateSubjectSettingsRoute: HttpRoutes[F] = Http4sServerInterpreter[F].toRoutes(updateSubjectSettingsEndpoint) {
    case (subject, settings) =>
      toRoute(storage.updateSubjectSettings(subject, settings.compatibilityType, settings.isLocked))
  }

  val subjectVersionsEndpoint: Endpoint[String, (StatusCode, ErrorInfo), List[Int], Any] =
    baseEndpoint.get.in("subjects").in(path[String]).in("versions").out(jsonBody[List[Int]])

  val subjectVersionsRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(subjectVersionsEndpoint)(subject => toRoute(storage.subjectVersions(subject)))

  val subjectSchemasMetadataEndpoint: Endpoint[String, (StatusCode, ErrorInfo), List[SubjectSchemaMetadata], Any] =
    baseEndpoint.get.in("subjects").in(path[String]).in("schemas").out(jsonBody[List[SubjectSchemaMetadata]])

  val subjectSchemasMetadataRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(subjectSchemasMetadataEndpoint)(subject =>
      toRoute(storage.subjectSchemasMetadata(subject))
    )

  val subjectSchemaByVersionEndpoint: Endpoint[(String, Int), (StatusCode, ErrorInfo), SubjectSchemaMetadata, Any] =
    baseEndpoint.get
      .in("subjects")
      .in(path[String])
      .in("versions")
      .in(path[Int].validate(Validator.min(1)))
      .out(jsonBody[SubjectSchemaMetadata])

  val subjectSchemaByVersionRoute: HttpRoutes[F] = Http4sServerInterpreter[F].toRoutes(subjectSchemaByVersionEndpoint) {
    case (subject, version) => toRoute(storage.subjectSchemaByVersion(subject, version))
  }

  val schemaByIdEndpoint: Endpoint[Int, (StatusCode, ErrorInfo), SchemaMetadata, Any] =
    baseEndpoint.get.in("schemas").in(path[Int].validate(Validator.min(1))).out(jsonBody[SchemaMetadata])

  val schemaByIdRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(schemaByIdEndpoint)(schemaId => toRoute(storage.schemaById(schemaId)))

  val schemaIdBySubjectAndSchemaEndpoint: Endpoint[(String, SchemaText), (StatusCode, ErrorInfo), SchemaId, Any] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("schemas" / "id")
      .in(jsonBody[SchemaText])
      .out(jsonBody[SchemaId])

  val schemaIdBySubjectAndSchemaRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(schemaIdBySubjectAndSchemaEndpoint) { case (subject, schemaText) =>
      toRoute(storage.schemaIdBySubjectAndSchema(subject, schemaText.getSchemaText))
    }

  val deleteSubjectEndpoint: Endpoint[String, (StatusCode, ErrorInfo), Boolean, Any] =
    baseEndpoint.delete.in("subjects").in(path[String]).out(jsonBody[Boolean])

  val deleteSubjectRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(deleteSubjectEndpoint)(subject => toRoute(storage.deleteSubject(subject)))

  val deleteSubjectSchemaByVersionEndpoint: Endpoint[(String, Int), (StatusCode, ErrorInfo), Boolean, Any] =
    baseEndpoint.delete
      .in("subjects")
      .in(path[String])
      .in("versions")
      .in(path[Int].validate(Validator.min(1)))
      .out(jsonBody[Boolean])

  val deleteSubjectSchemaByVersionRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(deleteSubjectSchemaByVersionEndpoint) { case (subject, version) =>
      toRoute(storage.deleteSubjectSchemaByVersion(subject, version))
    }

  val checkSubjectSchemaCompatibilityEndpoint: Endpoint[(String, SchemaText), (StatusCode, ErrorInfo), Boolean, Any] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("compatibility" / "schemas")
      .in(jsonBody[SchemaText])
      .out(jsonBody[Boolean])

  val checkSubjectSchemaCompatibilityRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(checkSubjectSchemaCompatibilityEndpoint) { case (subject, schemaText) =>
      toRoute(storage.checkSubjectSchemaCompatibility(subject, schemaText.getSchemaText))
    }

  val registerSchemaEndpoint: Endpoint[SchemaText, (StatusCode, ErrorInfo), SchemaId, Any] =
    baseEndpoint.post.in("schemas").in(jsonBody[SchemaText]).out(jsonBody[SchemaId])

  val registerSchemaRoute: HttpRoutes[F] = Http4sServerInterpreter[F].toRoutes(registerSchemaEndpoint)(schemaText =>
    toRoute(storage.registerSchema(schemaText.getSchemaText, schemaText.getSchemaType))
  )

  val registerSchemaAndSubjectEndpoint
    : Endpoint[(String, SubjectAndSchemaRequest), (StatusCode, ErrorInfo), SchemaId, Any] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("schemas")
      .in(jsonBody[SubjectAndSchemaRequest])
      .out(jsonBody[SchemaId])

  val registerSchemaAndSubjectRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(registerSchemaAndSubjectEndpoint) { case (subject, request) =>
      toRoute(
        storage
          .registerSchema(subject, request.getSchemaText, request.getCompatibilityType, request.getSchemaType)
          .handleErrorWith {
            case SubjectIsAlreadyConnectedToSchema(_, id) => SchemaId.instance(id).pure
            case e                                        => e.raiseError
          }
      )
    }

  val registerSubjectEndpoint: Endpoint[SubjectMetadata, (StatusCode, ErrorInfo), SubjectMetadata, Any] =
    baseEndpoint.post.in("subjects").in(jsonBody[SubjectMetadata]).out(jsonBody[SubjectMetadata])

  val registerSubjectRoute: HttpRoutes[F] =
    Http4sServerInterpreter[F].toRoutes(registerSubjectEndpoint)(subjectMetadata =>
      toRoute(
        storage
          .registerSubject(subjectMetadata.getSubject, subjectMetadata.getCompatibilityType, subjectMetadata.isLocked)
      )
    )

  val addSchemaToSubjectEndpoint: Endpoint[(String, Int), (StatusCode, ErrorInfo), Int, Any] =
    baseEndpoint.post
      .in("subjects")
      .in(path[String])
      .in("schemas")
      .in(path[Int].validate(Validator.min(1)))
      .out(jsonBody[Int])

  val addSchemaToSubjectRoute: HttpRoutes[F] = Http4sServerInterpreter[F].toRoutes(addSchemaToSubjectEndpoint) {
    case (subject, schemaId) => toRoute(storage.addSchemaToSubject(subject, schemaId))
  }

  val route: HttpRoutes[F] = subjectsRoute
    .combineK(subjectMetadataRoute)
    .combineK(updateSubjectSettingsRoute)
    .combineK(subjectVersionsRoute)
    .combineK(subjectSchemasMetadataRoute)
    .combineK(subjectSchemaByVersionRoute)
    .combineK(schemaByIdRoute)
    .combineK(schemaIdBySubjectAndSchemaRoute)
    .combineK(deleteSubjectRoute)
    .combineK(deleteSubjectSchemaByVersionRoute)
    .combineK(checkSubjectSchemaCompatibilityRoute)
    .combineK(registerSchemaRoute)
    .combineK(registerSchemaAndSubjectRoute)
    .combineK(registerSubjectRoute)
    .combineK(addSchemaToSubjectRoute)

  def toRoute[A](fa: F[A]): F[Either[(StatusCode, ErrorInfo), A]] =
    fa.map(_.asRight[(StatusCode, ErrorInfo)]).handleError(err => handleError(err).asLeft[A])

  def handleError(err: Throwable): (StatusCode, ErrorInfo) = err match {
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
  val apiVersion = "v2"

  def create[F[_]: Timer: Concurrent: ContextShift](service: Service[F]): SchemaKeeperApi[F] = new SchemaKeeperApi(
    service
  )
}
