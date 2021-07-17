package schemakeeper.server.storage.exception

class PostgreSQLExceptionHandler extends StorageExceptionHandler {
  override def isUniqueViolation(e: Throwable): Boolean = e match {
    case duplicate: org.postgresql.util.PSQLException =>
      if (duplicate.getSQLState == "23505") {
        true
      } else {
        false
      }
    case _ => false
  }
}

object PostgreSQLExceptionHandler {
  def apply(): PostgreSQLExceptionHandler = new PostgreSQLExceptionHandler()
}
