package schemakeeper.server.api.protocol

case class ErrorCode(code: Int)

object ErrorCode {
  def fromCode(code: Int): ErrorCode = code match {
    case 1000 => BackendErrorCode
    case 1001 => SubjectDoesNotExistCode
    case 1002 => SubjectIsAlreadyExistsCode
    case 1003 => SubjectHasNoRegisteredSchemasCode
    case 1004 => SubjectSchemaVersionDoesNotExistCode
    case 1005 => SchemaIdDoesNotExistCode
    case 1006 => ConfigIsNotDefinedCode
    case 1007 => SchemaIsNotValidCode
    case 1008 => SchemaIsAlreadyExistCode
    case 1009 => SubjectIsAlreadyConnectedToSchemaCode
    case 1010 => SchemaIsNotCompatibleCode
    case 1011 => SchemaIsNotRegisteredCode
    case 1012 => SubjectIsNotConnectedToSchemaCode
    case x => unknownCode(x)
  }

  def unknownCode(code: Int): ErrorCode = ErrorCode(code)

  val BackendErrorCode = ErrorCode(1000)
  val SubjectDoesNotExistCode = ErrorCode(1001)
  val SubjectIsAlreadyExistsCode = ErrorCode(1002)
  val SubjectHasNoRegisteredSchemasCode = ErrorCode(1003)
  val SubjectSchemaVersionDoesNotExistCode = ErrorCode(1004)
  val SchemaIdDoesNotExistCode = ErrorCode(1005)
  val ConfigIsNotDefinedCode = ErrorCode(1006)
  val SchemaIsNotValidCode = ErrorCode(1007)
  val SchemaIsAlreadyExistCode = ErrorCode(1008)
  val SubjectIsAlreadyConnectedToSchemaCode = ErrorCode(1009)
  val SchemaIsNotCompatibleCode = ErrorCode(1010)
  val SchemaIsNotRegisteredCode = ErrorCode(1011)
  val SubjectIsNotConnectedToSchemaCode = ErrorCode(1012)
}
