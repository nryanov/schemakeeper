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
      case Left(e) =>
        logger.error(s"Error while getting subjects: ${e.msg}")
        Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      case Right(v) => Ok(v)
    }
  }

  final val subjectMetadata: Endpoint[IO, SubjectMetadata] = get(apiVersion
    :: "subjects"
    :: path[String]) { subject: String =>
    logger.info(s"Get subject metadata: $subject")
    storage.subjectMetadata(subject).map {
      case Left(e) =>
        logger.error(s"Error while getting subject: $subject metadata: ${e.msg}")
        e match {
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
      case Left(e) =>
        logger.error(s"Error while getting subject versions: $subject metadata: ${e.msg}")
        e match {
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
      case Left(e) =>
        logger.error(s"Error while getting subject schemas: $subject metadata: ${e.msg}")
        e match {
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
    logger.info(s"Get subject schema by version: $subject - $version")
    storage.subjectSchemaByVersion(subject, version).map {
      case Left(e) =>
        logger.error(s"Error while getting subject: $subject schema by versions: $version metadata: ${e.msg}")
        e match {
          case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
          case s: SubjectSchemaVersionDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectSchemaVersionDoesNotExistCode), Status.NotFound)
          case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
        }
      case Right(v) => Ok(v)
    }
  }

  final val lockSubject: Endpoint[IO, Boolean] = post(apiVersion
    :: "subjects"
    :: path[String]
    :: "lock") { subject: String =>
    logger.info(s"Lock subjecT: $subject")
    storage.lockSubject(subject).map {
      case Left(e) =>
        logger.error(s"Error while locking subject: $subject: ${e.msg}")
        e match {
          case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
          case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
        }
      case Right(v) => Ok(v)
    }
  }

  final val unlockSubject: Endpoint[IO, Boolean] = post(apiVersion
    :: "subjects"
    :: path[String]
    :: "unlock") { subject: String =>
    logger.info(s"Lock subject: $subject")
    storage.unlockSubject(subject).map {
      case Left(e) =>
        logger.error(s"Error while unlocking subject: $subject: ${e.msg}")
        e match {
          case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
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
      case Left(e) =>
        logger.error(s"Error while getting schema by id: $id : ${e.msg}")
        e match {
          case s: SchemaIdDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIdDoesNotExistCode), Status.NotFound)
          case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
        }
      case Right(v) => Ok(v)
    }
  }

  final val schemaIdBySubjectAndSchema: Endpoint[IO, SchemaId] = post(apiVersion
    :: "subjects"
    :: path[String]
    :: "schemas"
    :: "id"
    :: jsonBody[SchemaText]) { (subject: String, schemaText: SchemaText) =>
    logger.info(s"Get schema id: $subject - ${schemaText.getSchemaText}")
    storage.schemaIdBySubjectAndSchema(subject, schemaText.getSchemaText).map {
      case Left(e) =>
        logger.error(s"Error while getting schema id by subject: $subject and schema: $schemaText : ${e.msg}")
        e match {
          case s: SchemaIsNotRegistered => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotRegisteredCode), Status.NotFound)
          case s: SchemaIsNotValid => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest)
          case s: SubjectIsNotConnectedToSchema => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectIsNotConnectedToSchemaCode), Status.BadRequest)
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
      case Left(e) =>
        logger.error(s"Error while deleting subject: $subject: ${e.msg}")
        Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
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
      case Left(e) =>
        logger.error(s"Error while deleting subject schema by version: $subject-$version: ${e.msg}")
        e match {
          case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
          case s: SubjectSchemaVersionDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectSchemaVersionDoesNotExistCode), Status.NotFound)
          case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
        }
      case Right(v) => Ok(v)
    }
  }

  final val checkSubjectSchemaCompatibility: Endpoint[IO, Boolean] = post(apiVersion
    :: "subjects"
    :: path[String]
    :: "compatibility"
    :: "schemas"
    :: jsonBody[SchemaText]) { (subject: String, schema: SchemaText) =>
    logger.info(s"Check subject schema compatibility: $subject - ${schema.getSchemaText}")
    storage.checkSubjectSchemaCompatibility(subject, schema.getSchemaText).map {
      case Left(e) =>
        logger.error(s"Error while checking subject schema compatibility: $subject-$schema: ${e.msg}")
        e match {
          case s: SchemaIsNotValid => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest)
          case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
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
      case Left(e) =>
        logger.error(s"Error while updating subject compatibility: $subject-${compatibilityType.identifier}: ${e.msg}")
        e match {
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
      case Left(e) =>
        logger.error(s"Error while getting subject compatibility: $subject: ${e.msg}")
        e match {
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
      case Left(e) =>
        logger.error(s"Error while registering schema: $schemaText: ${e.msg}")
        e match {
          case s: SchemaIsNotValid => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest)
          case s: SchemaIsAlreadyExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsAlreadyExistCode), Status.BadRequest)
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
      case Left(e) =>
        logger.error(s"Error while registering schema and subject: $request: ${e.msg}")
        e match {
          case s: SchemaIsNotValid => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotValidCode), Status.BadRequest)
          case _@SubjectIsAlreadyConnectedToSchema(_, schemaId) => Ok(SchemaId.instance(schemaId))
          case s: SchemaIsNotCompatible => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIsNotCompatibleCode), Status.BadRequest)
          case s: SubjectIsLocked => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectIsLockedErrorCode), Status.BadRequest)
          case e: SchemaKeeperError => Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
        }
      case Right(v) => Ok(v)
    }
  }

  final val registerSubject: Endpoint[IO, SubjectMetadata] = put(apiVersion
    :: "subjects"
    :: jsonBody[SubjectMetadata]) { meta: SubjectMetadata =>
    logger.info(s"Register new subject: $meta")
    storage.registerSubject(meta.getSubject, meta.getCompatibilityType).map {
      case Left(e) =>
        logger.error(s"Error while registering subject: $meta: ${e.msg}")
        e match {
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
      case Left(e) =>
        logger.error(s"Error while adding schema: $id to subject: $subject: ${e.msg}")
        e match {
          case s: SchemaIdDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SchemaIdDoesNotExistCode), Status.NotFound)
          case s: SubjectDoesNotExist => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
          case s: SubjectIsLocked => Output.failure(ErrorInfo(s.msg, ErrorCode.SubjectIsLockedErrorCode), Status.BadRequest)
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
      case Left(e) =>
        logger.error(s"Error while checking subject existencee: $subject: ${e.msg}")
        Output.failure(ErrorInfo(e.msg, ErrorCode.BackendErrorCode), Status.InternalServerError)
      case Right(v) => Ok(v)
    }
  }
}

object SchemaKeeperApi {
  private val logger = LoggerFactory.getLogger(SchemaKeeperApi.getClass)
  private val apiVersion = "v1"

  def apply(service: Service[IO])(implicit S: ContextShift[IO]): SchemaKeeperApi = new SchemaKeeperApi(service)
}
