package schemakeeper.server.api

import io.finch._
import io.finch.circe._
import cats.effect.{ContextShift, IO}
import io.circe.generic.auto._
import org.slf4j.LoggerFactory
import Validation._
import SchemaKeeperApi._
import schemakeeper.api._
import schemakeeper.server.service.Service
import schemakeeper.server.api.protocol.JsonProtocol._


class SchemaKeeperApi(storage: Service[IO])(implicit S: ContextShift[IO]) extends Endpoint.Module[IO] {

  final val schema: Endpoint[IO, SchemaResponse] = get(apiVersion
    :: "schema"
    :: path[Int].should(positiveSchemaId)) { id: Int =>
    logger.info("Getting schema by id: {}", id)
    storage.schemaById(id).map {
      case Some(s) => Ok(SchemaResponse.instance(s))
      case None => NoContent[SchemaResponse]
    }
  }

  final val subjects: Endpoint[IO, List[String]] = get(apiVersion
    :: "subjects") {
    logger.info("Getting subject list")
    storage.subjects().map(Ok)
  }

  final val subjectVersions: Endpoint[IO, List[Int]] = get(apiVersion
    :: "subjects"
    :: path[String]
    :: "versions") { subjectName: String =>
    logger.info("Getting subject versions: {}", subjectName)
    storage.subjectVersions(subjectName).map {
      case l if l.isEmpty => NoContent[List[Int]]
      case l => Ok(l)
    }
  }

  final val subjectSchemaByVersion: Endpoint[IO, SchemaMetadata] = get(apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)) { (subjectName: String, version: Int) =>
    logger.info("Getting subject schema metadata by version: {}:{}", subjectName, version)
    storage.subjectSchemaByVersion(subjectName, version).map {
      case Some(meta) => Ok(meta)
      case None => NoContent[SchemaMetadata]
    }
  }

  final val subjectOnlySchemaByVersion: Endpoint[IO, SchemaResponse] = get(apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)
    :: "schema") { (subjectName: String, version: Int) =>
    logger.info("Getting only subject schema by version: {}:{}", subjectName, version)
    storage.subjectOnlySchemaByVersion(subjectName, version).map {
      case Some(meta) => Ok(SchemaResponse.instance(meta))
      case None => NoContent[SchemaResponse]
    }
  }

  final val deleteSubject: Endpoint[IO, Boolean] = delete(apiVersion
    :: "subjects"
    :: path[String]) { subjectName: String =>
    logger.info("Deleting subject: {}", subjectName)
    storage.deleteSubject(subjectName).map(Ok)
  }

  final val deleteSubjectVersion: Endpoint[IO, Boolean] = delete(apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)) { (subjectName: String, versionId: Int) =>
    logger.info("Deleting subject version: {}:{}", subjectName, versionId)
    storage.deleteSubjectVersion(subjectName, versionId).map(Ok)
  }

  final val registerNewSubjectSchema: Endpoint[IO, SchemaId] = post(apiVersion
    :: "subjects"
    :: path[String]
    :: jsonBody[SchemaRequest]) { (subjectName: String, schema: SchemaRequest) =>
    logger.info(s"Register new subject schema: $subjectName - $schema")
    storage.registerNewSubjectSchema(subjectName, schema.getSchemaText)
      .map(SchemaId.instance)
      .map(Ok)
  }

  final val checkSubjectSchemaCompatibility: Endpoint[IO, Boolean] = post(apiVersion
    :: "compatibility"
    :: path[String]
    :: jsonBody[SchemaRequest]) { (subjectName: String, schema: SchemaRequest) =>
    logger.info(s"Check subject schema compatibility: $subjectName - ${schema.getSchemaText}")
    storage.checkSubjectSchemaCompatibility(subjectName, schema.getSchemaText).map(Ok)
  }

  final val updateSubjectCompatibilityConfig: Endpoint[IO, CompatibilityTypeMetadata] = put(apiVersion
    :: "compatibility"
    :: path[String]
    :: jsonBody[CompatibilityTypeMetadata]) { (subjectName: String, compatibility: CompatibilityTypeMetadata) =>
    logger.info(s"Update compatibility level for subject: $subjectName - $compatibility")
    storage.updateSubjectCompatibility(subjectName, compatibility.getCompatibilityType).map {
      case Some(v) => Ok(CompatibilityTypeMetadata.instance(v))
      case None => NoContent[CompatibilityTypeMetadata]
    }
  }

  final val getSubjectCompatibilityConfig: Endpoint[IO, CompatibilityTypeMetadata] = get(apiVersion
    :: "compatibility"
    :: path[String]) { subjectName: String =>
    logger.info("Get subject compatibility level: {}", subjectName)
    storage.getSubjectCompatibility(subjectName)
      .map {
        case Some(v) => Ok(CompatibilityTypeMetadata.instance(v))
        case None => NoContent[CompatibilityTypeMetadata]
      }
  }

  final val getGlobalCompatibilityConfig: Endpoint[IO, CompatibilityTypeMetadata] = get(apiVersion
    :: "compatibility") {
    logger.info("Get global compatibility config")
    storage.getGlobalCompatibility()
      .map {
        case Some(v) => Ok(CompatibilityTypeMetadata.instance(v))
        case None => NoContent[CompatibilityTypeMetadata]
      }
  }

  final val updateGlobalCompatibilityConfig: Endpoint[IO, CompatibilityTypeMetadata] = put(apiVersion
    :: "compatibility"
    :: jsonBody[CompatibilityTypeMetadata]) { compatibility: CompatibilityTypeMetadata =>
    logger.info(s"Update global compatibility config: ${compatibility.getCompatibilityType.identifier}")
    storage.updateGlobalCompatibility(compatibility.getCompatibilityType)
      .map {
        case Some(v) => Ok(CompatibilityTypeMetadata.instance(v))
        case None => NoContent[CompatibilityTypeMetadata]
      }
  }
}

object SchemaKeeperApi {
  private val logger = LoggerFactory.getLogger(SchemaKeeperApi.getClass)
  private val apiVersion = "v1"

  def apply(service: Service[IO])(implicit S: ContextShift[IO]): SchemaKeeperApi = new SchemaKeeperApi(service)
}
