package schemakeeper.server.storage.exception

class H2ExceptionHandler extends StorageExceptionHandler {
  override def isRecoverable(e: Throwable): Boolean = e match {
    case duplicate: org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException =>
      if (duplicate.getErrorCode == 23505) {
        true
      } else {
        false
      }
    case _ => false
  }
}

object H2ExceptionHandler {
  def apply(): H2ExceptionHandler = new H2ExceptionHandler()
}