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
import schemakeeper.api.{CompatibilityTypeMetadata, SchemaMetadata}
import schemakeeper.schema.CompatibilityType
import schemakeeper.server.api.protocol.JsonProtocol._

//todo: Use Sync?
class SchemaKeeperApi(storage: Service[Id]) {
  val schema = get(
    /
      :: apiVersion
      :: "schema"
      :: path[Int].should(positiveSchemaId)
  ) { id: Int =>
    logger.info("Getting schema by id: {}", id)
    FuturePool.unboundedPool {
      Ok(storage.schemaById(id))
    }
  }.mapOutput {
    case Some(s) => Ok(s)
    case None => NoContent[String]
  }

  val subjects = get(
    /
      :: apiVersion
      :: "subjects"
  ) {
    logger.info("Getting subject list")
    FuturePool.unboundedPool {
      Ok(storage.subjects())
    }
  }

  val subjectVersions = get(
    /
      :: apiVersion
      :: "subjects"
      :: path[String]
      :: "versions"
  ) { subjectName: String =>
    logger.info("Getting subject versions: {}", subjectName)
    FuturePool.unboundedPool {
      Ok(storage.subjectVersions(subjectName))
    }
  }.mapOutput {
    case l if l.isEmpty => NoContent[List[Int]]
    case l => Ok(l)
  }

  val subjectSchemaByVersion = get(
    /
      :: apiVersion
      :: "subjects"
      :: path[String]
      :: "versions"
      :: path[Int].should(positiveVersion)
  ) { (subjectName: String, version: Int) =>
    logger.info("Getting subject schema metadata by version: {}:{}", subjectName, version)
    FuturePool.unboundedPool {
      Ok(storage.subjectSchemaByVersion(subjectName, version))
    }
  }.mapOutput {
    case Some(meta) => Ok(meta)
    case None => NoContent[SchemaMetadata]
  }

  val subjectOnlySchemaByVersion = get(
    /
      :: apiVersion
      :: "subjects"
      :: path[String]
      :: "versions"
      :: path[Int].should(positiveVersion)
      :: "schema"
  ) { (subjectName: String, version: Int) =>
    logger.info("Getting only subject schema by version: {}:{}", subjectName, version)
    FuturePool.unboundedPool {
      Ok(storage.subjectOnlySchemaByVersion(subjectName, version))
    }
  }.mapOutput {
    case Some(meta) => Ok(meta)
    case None => NoContent[String]
  }

  val deleteSubject = delete(
    /
      :: apiVersion
      :: "subjects"
      :: path[String]
  ) { subjectName: String =>
    logger.info("Deleting subject: {}", subjectName)
    FuturePool.unboundedPool {
      Ok(storage.deleteSubject(subjectName))
    }
  }

  val deleteSubjectVersion = delete(
    /
      :: apiVersion
      :: "subjects"
      :: path[String]
      :: "versions"
      :: path[Int].should(positiveVersion)
  ) { (subjectName: String, versionId: Int) =>
    logger.info("Deleting subject version: {}:{}", subjectName, versionId)
    FuturePool.unboundedPool {
      Ok(storage.deleteSubjectVersion(subjectName, versionId))
    }
  }

  val registerNewSubjectSchema = post(
    /
      :: apiVersion
      :: "subjects"
      :: path[String]
      :: stringBody
  ) { (subjectName: String, schema: String) =>
    logger.info(s"Register new subject schema: $subjectName - $schema")
    FuturePool.unboundedPool {
      Ok(storage.registerNewSubjectSchema(subjectName, schema))
    }
  }

  val checkSubjectSchemaCompatibility = post(
    /
      :: apiVersion
      :: "compatibility"
      :: path[String]
      :: stringBody
  ) { (subjectName: String, schema: String) =>
    logger.info(s"Check subject schema compatibility: $subjectName - $schema")
    FuturePool.unboundedPool {
      Ok(storage.checkSubjectSchemaCompatibility(subjectName, schema))
    }
  }

  val updateSubjectCompatibilityConfig = put(
    /
      :: apiVersion
      :: "compatibility"
      :: path[String]
      :: jsonBody[CompatibilityTypeMetadata]
  ) { (subjectName: String, compatibility: CompatibilityTypeMetadata) =>
    logger.info(s"Update compatibility level for subject: $subjectName - $compatibility")
    FuturePool.unboundedPool {
      Ok(storage.updateSubjectCompatibility(subjectName, compatibility.getCompatibilityType))
    }
  }.mapOutput {
    case Some(v) => Ok(v)
    case None => NoContent[CompatibilityType]
  }

  val getSubjectCompatibilityConfig = get(
    /
      :: apiVersion
      :: "compatibility"
      :: path[String]
  ) { subjectName: String =>
    logger.info("Get subject compatibility level: {}", subjectName)
    FuturePool.unboundedPool {
      Ok(storage.getSubjectCompatibility(subjectName))
    }
  }.mapOutput {
    case Some(v) => Ok(v)
    case None => NoContent[CompatibilityType]
  }
}

object SchemaKeeperApi {
  private val logger = LoggerFactory.getLogger(SchemaKeeperApi.getClass)
  val apiVersion = "v1"

  def apply(service: Service[Id]): SchemaKeeperApi = new SchemaKeeperApi(service)
}
