package schemakeeper.server.api.protocol

case class ErrorInfo(reason: String, code: ErrorCode) extends Exception(reason)
