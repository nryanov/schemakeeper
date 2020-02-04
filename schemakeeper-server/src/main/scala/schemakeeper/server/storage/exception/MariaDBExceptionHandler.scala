package schemakeeper.server.storage.exception

class MariaDBExceptionHandler extends StorageExceptionHandler {
  override def isRecoverable(e: Throwable): Boolean = e match {
    case duplicate: java.sql.SQLIntegrityConstraintViolationException =>
      if (duplicate.getErrorCode == 1062) {
        true
      } else {
        false
      }
    case _ => false
  }
}

object MariaDBExceptionHandler {
  def apply(): MariaDBExceptionHandler = new MariaDBExceptionHandler()
}
