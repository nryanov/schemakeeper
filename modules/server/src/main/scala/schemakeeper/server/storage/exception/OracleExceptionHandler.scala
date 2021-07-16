package schemakeeper.server.storage.exception

class OracleExceptionHandler extends StorageExceptionHandler {
  override def isUniqueViolation(e: Throwable): Boolean = e match {
    case duplicate: java.sql.SQLIntegrityConstraintViolationException =>
      if (duplicate.getErrorCode == 1062) {
        true
      } else {
        false
      }
    case _ => false
  }
}

object OracleExceptionHandler {
  def apply(): OracleExceptionHandler = new OracleExceptionHandler()
}
