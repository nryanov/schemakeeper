package schemakeeper.server.api

import io.finch._
import io.finch.circe._
import org.slf4j.LoggerFactory
import cats.effect.{ContextShift, IO}
import com.twitter.finagle.http.Status
import Validation._
import schemakeeper.api._
import schemakeeper.schema.CompatibilityType
import schemakeeper.server.api.protocol.{ErrorCode, ErrorInfo}
import schemakeeper.server.service._
import schemakeeper.server.api.protocol.JsonProtocol._

class SchemaKeeperApi(storage: Service[IO])(implicit S: ContextShift[IO]) extends Endpoint.Module[IO] {

  import SchemaKeeperApi._

  final val subjects: Endpoint[IO, List[String]] = get(apiVersion
    :: "subjects") {
    logger.info("Get subjects list")
    storage.subjects().map {
      case Left(e) => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      case Right(v) => Ok(v)
    }
  }

  final val subjectMetadata: Endpoint[IO, SubjectMetadata] = get(apiVersion
    :: "subjects"
    :: path[String]) { subject: String =>
    logger.info(s"Get subject metadata: $subject")
    storage.subjectMetadata(subject).map {
      case Left(e) => e match {
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val subjectVersions: Endpoint[IO, List[Int]] = get(apiVersion
    :: "subjects"
    :: path[String]
    :: "versions") { subject: String =>
    logger.info(s"Get subject: $subject versions")
    storage.subjectVersions(subject).map {
      case Left(e) => e match {
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val subjectSchemasMetadata: Endpoint[IO, List[SubjectSchemaMetadata]] = get(apiVersion
    :: "subjects"
    :: path[String]
    :: "schemas") { subject: String =>
    logger.info(s"Get subject: $subject schemas metadata")
    storage.subjectSchemasMetadata(subject).map {
      case Left(e) => e match {
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val subjectSchemaByVersion: Endpoint[IO, SubjectSchemaMetadata] = get(apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)) { (subject: String, version: Int) =>
    logger.info(s"Get subject  schema by version: $subject - $version")
    storage.subjectSchemaByVersion(subject, version).map {
      case Left(e) => e match {
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
        case s: SubjectSchemaVersionDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectSchemaVersionDoesNotExistCode), Status.NotFound)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val schemaById: Endpoint[IO, SchemaMetadata] = get(apiVersion
    :: "schemas"
    :: path[Int].should(positiveSchemaId)) { id: Int =>
    logger.info(s"Get schema by id: $id")
    storage.schemaById(id).map {
      case Left(e) => e match {
        case s: SchemaDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaDoesNotExistCode), Status.NotFound)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val deleteSubject: Endpoint[IO, Boolean] = delete(apiVersion
    :: "subjects"
    :: path[String]) { subject: String =>
    logger.info(s"Delete subject: $subject")
    storage.deleteSubject(subject).map {
      case Left(e) => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      case Right(v) => Ok(v)
    }
  }

  final val deleteSubjectSchemaByVersion: Endpoint[IO, Boolean] = delete(apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)) { (subject: String, version: Int) =>
    logger.info(s"Delete subject schema by version: $subject - $version")
    storage.deleteSubjectSchemaByVersion(subject, version).map {
      case Left(e) => e match {
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.BadRequest)
        case s: SubjectSchemaVersionDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectSchemaVersionDoesNotExistCode), Status.BadRequest)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val checkSubjectSchemaCompatibility: Endpoint[IO, Boolean] = post(apiVersion
    :: "subjects"
    :: path[String]
    :: "schemas"
    :: jsonBody[SchemaText]) { (subject: String, schema: SchemaText) =>
    logger.info(s"Check subject schema compatibility: $subject - ${schema.getSchemaText}")
    storage.checkSubjectSchemaCompatibility(subject, schema.getSchemaText).map {
      case Left(e) => e match {
        case s: SchemaIsNotValid => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest)
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.BadRequest)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val updateSubjectCompatibility: Endpoint[IO, Boolean] = post(apiVersion
    :: "subjects"
    :: path[String]
    :: "compatibility"
    :: jsonBody[CompatibilityType]) { (subject: String, compatibilityType: CompatibilityType) =>
    logger.info(s"Update subject compatibility: $subject - ${compatibilityType.identifier}")
    storage.updateSubjectCompatibility(subject, compatibilityType).map {
      case Left(e) => e match {
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val getSubjectCompatibility: Endpoint[IO, CompatibilityType] = get(apiVersion
    :: "subjects"
    :: path[String]
    :: "compatibility") { subject: String =>
    logger.info(s"Get subject compatibility type: $subject")
    storage.getSubjectCompatibility(subject).map {
      case Left(e) => e match {
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val registerSchema: Endpoint[IO, SchemaId] = put(apiVersion
    :: "schemas"
    :: jsonBody[SchemaText]) { schemaText: SchemaText =>
    logger.info(s"Register new schema: $schemaText")
    storage.registerSchema(schemaText.getSchemaText, schemaText.getSchemaType).map {
      case Left(e) => e match {
        case s: SchemaIsNotValid => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val registerSchemaAndSubject: Endpoint[IO, SchemaId] = put(apiVersion
    :: "subjects"
    :: path[String]
    :: "schemas"
    :: jsonBody[SubjectAndSchemaRequest]) { (subject: String, request: SubjectAndSchemaRequest) =>
    logger.info(s"Register schema and subject: $subject - $request")
    storage.registerSchema(subject, request.getSchemaText, request.getCompatibilityType, request.getSchemaType).map {
      case Left(e) => e match {
        case s: SchemaIsNotValid => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest)
        case _@SubjectIsAlreadyConnectedToSchema(_, schemaId) => Ok(SchemaId.instance(schemaId))
        case s: SchemaIsNotCompatible => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotCompatibleCode), Status.BadRequest)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val registerSubject: Endpoint[IO, Unit] = put(apiVersion
    :: "subjects"
    :: jsonBody[SubjectMetadata]) { meta: SubjectMetadata =>
    logger.info(s"Register new subject: $meta")
    storage.registerSubject(meta.getSubject, meta.getCompatibilityType).map {
      case Left(e) => e match {
        case s: SubjectIsAlreadyExists => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectIsAlreadyExistsCode), Status.BadRequest)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val addSchemaToSubject: Endpoint[IO, Int] = put(apiVersion
    :: "subjects"
    :: path[String]
    :: "schemas"
    :: path[Int].should(positiveSchemaId)) { (subject: String, id: Int) =>
    logger.info(s"Connect subject: $subject and schema: $id")
    storage.addSchemaToSubject(subject, id).map {
      case Left(e) => e match {
        case s: SchemaDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaDoesNotExistCode), Status.NotFound)
        case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
        case s: SubjectIsAlreadyConnectedToSchema => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectIsAlreadyConnectedToSchemaCode), Status.BadRequest)
        case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      }
      case Right(v) => Ok(v)
    }
  }

  final val isSubjectExist: Endpoint[IO, Boolean] = post(apiVersion
    :: "subjects"
    :: path[String]) { subject: String =>
    logger.info(s"Check if subject: $subject exists")
    storage.isSubjectExist(subject).map {
      case Left(e) => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      case Right(v) => Ok(v)
    }
  }

  final val getGlobalCompatibility: Endpoint[IO, CompatibilityType] = get(apiVersion
    :: "compatibility") {
    logger.info("Get global compatibility type")
    storage.getGlobalCompatibility().map {
      case Left(e) => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      case Right(v) => Ok(v)
    }
  }

  final val updateGlobalCompatibility: Endpoint[IO, Boolean] = post(apiVersion
    :: "compatibility"
    :: jsonBody[CompatibilityType]) { compatibilityType: CompatibilityType =>
    logger.info(s"Update global compatibility type: ${compatibilityType.identifier}")
    storage.updateGlobalCompatibility(compatibilityType).map {
      case Left(e) => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      case Right(v) => Ok(v)
    }
  }
}

object SchemaKeeperApi {
  private val logger = LoggerFactory.getLogger(SchemaKeeperApi.getClass)
  private val apiVersion = "v1"

  def apply(service: Service[IO])(implicit S: ContextShift[IO]): SchemaKeeperApi = new SchemaKeeperApi(service)
}
