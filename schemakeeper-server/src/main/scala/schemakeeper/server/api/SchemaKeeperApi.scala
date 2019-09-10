package schemakeeper.server.api

import io.finch._
import io.finch.syntax._
import io.finch.circe._
import com.twitter.util.FuturePool
import org.slf4j.LoggerFactory
import schemakeeper.server.service.Service
import SchemaKeeperApi._
import Validation._
import cats.Id
import schemakeeper.api._
import schemakeeper.server.api.protocol.JsonProtocol._

//todo: Use Sync?
class SchemaKeeperApi(storage: Service[Id]) {
  val schema: Endpoint[SchemaResponse] = get(/
    :: apiVersion
    :: "schema"
    :: path[Int].should(positiveSchemaId)) { id: Int =>
    logger.info("Getting schema by id: {}", id)
    FuturePool.unboundedPool {
      Ok(storage.schemaById(id))
    }
  }.mapOutput {
    case Some(s) => Ok(SchemaResponse.instance(s))
    case None => NoContent[SchemaResponse]
  }

  val subjects: Endpoint[List[String]] = get(/
    :: apiVersion
    :: "subjects") {
    logger.info("Getting subject list")
    FuturePool.unboundedPool {
      Ok(storage.subjects())
    }
  }

  val subjectVersions: Endpoint[List[Int]] = get(/
    :: apiVersion
    :: "subjects"
    :: path[String]
    :: "versions") { subjectName: String =>
    logger.info("Getting subject versions: {}", subjectName)
    FuturePool.unboundedPool {
      Ok(storage.subjectVersions(subjectName))
    }
  }.mapOutput {
    case l if l.isEmpty => NoContent[List[Int]]
    case l => Ok(l)
  }

  val subjectSchemaByVersion: Endpoint[SchemaMetadata] = get(/
    :: apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)) { (subjectName: String, version: Int) =>
    logger.info("Getting subject schema metadata by version: {}:{}", subjectName, version)
    FuturePool.unboundedPool {
      Ok(storage.subjectSchemaByVersion(subjectName, version))
    }
  }.mapOutput {
    case Some(meta) => Ok(meta)
    case None => NoContent[SchemaMetadata]
  }

  val subjectOnlySchemaByVersion: Endpoint[SchemaResponse] = get(/
    :: apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)
    :: "schema") { (subjectName: String, version: Int) =>
    logger.info("Getting only subject schema by version: {}:{}", subjectName, version)
    FuturePool.unboundedPool {
      Ok(storage.subjectOnlySchemaByVersion(subjectName, version))
    }
  }.mapOutput {
    case Some(meta) => Ok(SchemaResponse.instance(meta))
    case None => NoContent[SchemaResponse]
  }

  val deleteSubject: Endpoint[Boolean] = delete(/
    :: apiVersion
    :: "subjects"
    :: path[String]) { subjectName: String =>
    logger.info("Deleting subject: {}", subjectName)
    FuturePool.unboundedPool {
      Ok(storage.deleteSubject(subjectName))
    }
  }

  val deleteSubjectVersion: Endpoint[Boolean] = delete(/
    :: apiVersion
    :: "subjects"
    :: path[String]
    :: "versions"
    :: path[Int].should(positiveVersion)) { (subjectName: String, versionId: Int) =>
    logger.info("Deleting subject version: {}:{}", subjectName, versionId)
    FuturePool.unboundedPool {
      Ok(storage.deleteSubjectVersion(subjectName, versionId))
    }
  }

  val registerNewSubjectSchema: Endpoint[SchemaId] = post(/
    :: apiVersion
    :: "subjects"
    :: path[String]
    :: jsonBody[SchemaRequest]) { (subjectName: String, schema: SchemaRequest) =>
    logger.info(s"Register new subject schema: $subjectName - $schema")
    FuturePool.unboundedPool {
      Ok(storage.registerNewSubjectSchema(subjectName, schema.getSchemaText))
    }
  }.map(SchemaId.instance)

  val checkSubjectSchemaCompatibility: Endpoint[Boolean] = post(/
    :: apiVersion
    :: "compatibility"
    :: path[String]
    :: jsonBody[SchemaRequest]) { (subjectName: String, schema: SchemaRequest) =>
    logger.info(s"Check subject schema compatibility: $subjectName - ${schema.getSchemaText}")
    FuturePool.unboundedPool {
      Ok(storage.checkSubjectSchemaCompatibility(subjectName, schema.getSchemaText))
    }
  }

  val updateSubjectCompatibilityConfig: Endpoint[CompatibilityTypeMetadata] = put(/
    :: apiVersion
    :: "compatibility"
    :: path[String]
    :: jsonBody[CompatibilityTypeMetadata]) { (subjectName: String, compatibility: CompatibilityTypeMetadata) =>
    logger.info(s"Update compatibility level for subject: $subjectName - $compatibility")
    FuturePool.unboundedPool {
      Ok(storage.updateSubjectCompatibility(subjectName, compatibility.getCompatibilityType))
    }
  }.mapOutput {
    case Some(v) => Ok(CompatibilityTypeMetadata.instance(v))
    case None => NoContent[CompatibilityTypeMetadata]
  }

  val getSubjectCompatibilityConfig: Endpoint[CompatibilityTypeMetadata] = get(/
    :: apiVersion
    :: "compatibility"
    :: path[String]) { subjectName: String =>
    logger.info("Get subject compatibility level: {}", subjectName)
    FuturePool.unboundedPool {
      Ok(storage.getSubjectCompatibility(subjectName))
    }
  }.mapOutput {
    case Some(v) => Ok(CompatibilityTypeMetadata.instance(v))
    case None => NoContent[CompatibilityTypeMetadata]
  }
}

object SchemaKeeperApi {
  private val logger = LoggerFactory.getLogger(SchemaKeeperApi.getClass)
  val apiVersion = "v1"

  def apply(service: Service[Id]): SchemaKeeperApi = new SchemaKeeperApi(service)
}
