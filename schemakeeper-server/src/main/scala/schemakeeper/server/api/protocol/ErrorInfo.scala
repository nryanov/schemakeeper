package schemakeeper.server.api.protocol

final case class ErrorInfo(reason: String, code: ErrorCode) extends Exception(reason)
