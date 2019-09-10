package schemakeeper.server.service

import cats.Id
import javax.annotation.concurrent.NotThreadSafe
import org.apache.avro.Schema
import schemakeeper.api.{SchemaMetadata, SubjectMetadata}
import schemakeeper.schema.{AvroSchemaCompatibility, AvroSchemaUtils, CompatibilityType}

import scala.collection.mutable

// mutable state
case class InitialData(
                        schemaId: mutable.Map[SchemaMetadata, Int],
                        idSchema: mutable.Map[Int, SchemaMetadata],
                        subjectSchemaVersion: mutable.Map[String, mutable.Map[Int, SchemaMetadata]],
                        subjectMetadata: mutable.Map[String, SubjectMetadata]
                      )

@NotThreadSafe
class MockService(data: InitialData) extends Service[Id] {
  override def subjects(): Id[List[String]] =
    data.subjectSchemaVersion.keys.toList

  override def subjectVersions(subject: String): Id[List[Int]] =
    data.subjectSchemaVersion.get(subject).map(_.keys.toList).getOrElse(List.empty[Int])

  override def subjectSchemaByVersion(subject: String, version: Int): Id[Option[SchemaMetadata]] =
    data.subjectSchemaVersion.get(subject).flatMap(_.get(version))

  override def subjectOnlySchemaByVersion(subject: String, version: Int): Id[Option[String]] =
    data.subjectSchemaVersion.get(subject).flatMap(_.get(version).map(_.getSchemaText))

  override def schemaById(id: Int): Id[Option[String]] =
    data.idSchema.get(id).map(_.getSchemaText)

  override def deleteSubject(subject: String): Id[Boolean] = {
    data.subjectSchemaVersion.get(subject)
      .map(_.values)
      .getOrElse(Iterable.empty)
      .foreach(meta => {
        data.idSchema.remove(meta.getId)
        data.schemaId.remove(meta)
      })

    data.subjectSchemaVersion.remove(subject).isDefined
  }

  override def deleteSubjectVersion(subject: String, version: Int): Id[Boolean] = {
    val s = data.subjectSchemaVersion.get(subject).flatMap(_.find {
      case (v, _) => v == version
    })

    data.subjectSchemaVersion.get(subject).map(_.remove(s.map(_._1).getOrElse(-1)))
    s.foreach {
      case (_, meta) =>
        data.schemaId.remove(meta)
        data.idSchema.remove(meta.getId)
    }

    s.isDefined
  }

  override def checkSubjectSchemaCompatibility(subject: String, schema: String): Id[Boolean] = (for {
    lastSchemaMeta <- data.subjectSchemaVersion.get(subject).flatMap(_.values.lastOption)
    compatibilityType <- data.subjectMetadata.get(subject).map(_.getCompatibilityType)
    currentSchema = AvroSchemaUtils.parseSchema(lastSchemaMeta.getSchemaText)
    newSchema = AvroSchemaUtils.parseSchema(schema)
  } yield isSchemaCompatible(newSchema, currentSchema, compatibilityType)).getOrElse(false)

  override def updateSubjectCompatibility(subject: String, compatibilityType: CompatibilityType): Id[Option[CompatibilityType]] =
    data.subjectMetadata.get(subject).flatMap(meta => {
      meta.setCompatibilityType(compatibilityType)
      data.subjectMetadata.update(subject, meta)
      Some(compatibilityType)
    })

  override def getSubjectCompatibility(subject: String): Id[Option[CompatibilityType]] =
    data.subjectMetadata.get(subject).map(_.getCompatibilityType)

  override def getLastSchema(subject: String): Id[Option[String]] =
    data.subjectSchemaVersion.get(subject).flatMap(_.lastOption.map(_._2.getSchemaText))

  override def getLastSchemas(subject: String): Id[List[String]] =
    data.subjectSchemaVersion.get(subject).map(_.map(_._2.getSchemaText).toList).getOrElse(List.empty)

  override def registerNewSubjectSchema(subject: String, schema: String): Id[Int] = {
    val nextId = data.idSchema.keys.lastOption.getOrElse(0) + 1
    val nextVersion = data.subjectSchemaVersion.getOrElse(subject, Map.empty[Int, SchemaMetadata]).keys.lastOption.getOrElse(0) + 1

    data.idSchema.put(nextId, SchemaMetadata.instance(subject, nextId, nextVersion, schema))
    data.schemaId.put(SchemaMetadata.instance(subject, nextId, nextVersion, schema), nextId)
    data.subjectSchemaVersion.getOrElseUpdate(subject, new mutable.HashMap[Int, SchemaMetadata]())
      .put(nextVersion, SchemaMetadata.instance(subject, nextId, nextVersion, schema))

    nextId
  }

  private def isSchemaCompatible(newSchema: Schema, previousSchema: Schema, compatibilityType: CompatibilityType): Boolean = compatibilityType match {
    case CompatibilityType.NONE => true
    case CompatibilityType.BACKWARD => AvroSchemaCompatibility.BACKWARD_VALIDATOR.isCompatible(newSchema, previousSchema)
    case CompatibilityType.FORWARD => AvroSchemaCompatibility.FORWARD_VALIDATOR.isCompatible(newSchema, previousSchema)
    case CompatibilityType.FULL => AvroSchemaCompatibility.FULL_VALIDATOR.isCompatible(newSchema, previousSchema)
    case _ => false // do not check transitive compatibility types here
  }
}

object MockService {
  def apply(data: InitialData): MockService = new MockService(data)
}

object InitialDataGenerator {
  def apply(s: Seq[(String, String)] = Seq()): InitialData = {
    var id = 0
    var version = 0

    val schemaId: mutable.Map[SchemaMetadata, Int] = new mutable.HashMap()
    val idSchema: mutable.Map[Int, SchemaMetadata] = new mutable.HashMap()
    val subjectSchemaVersion: mutable.Map[String, mutable.Map[Int, SchemaMetadata]] = new mutable.LinkedHashMap()
    val subjectMetadata: mutable.Map[String, SubjectMetadata] = new mutable.HashMap()

    s.foreach {
      case (subject, schemaString) =>
        id = id + 1
        version = version + 1
        val meta = SchemaMetadata.instance(subject, id, version, schemaString)
        schemaId.put(meta, id)
        idSchema.put(id, meta)
        subjectSchemaVersion.getOrElseUpdate(subject, new mutable.LinkedHashMap()).put(version, meta)
    }

    s.toMap.keys.foreach(subject => subjectMetadata.put(subject, SubjectMetadata.instance(subject, CompatibilityType.NONE, "avro")))

    InitialData(schemaId, idSchema, subjectSchemaVersion, subjectMetadata)
  }
}