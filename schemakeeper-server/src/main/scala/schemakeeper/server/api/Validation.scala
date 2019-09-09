package schemakeeper.server.api

import io.finch._

object Validation {
  val positiveVersion: ValidationRule[Int] = ValidationRule[Int]("Version number should be positive") { _ > 0}
  val positiveSchemaId: ValidationRule[Int] = ValidationRule[Int]("Schema id should be positive") { _ > 0}
}
