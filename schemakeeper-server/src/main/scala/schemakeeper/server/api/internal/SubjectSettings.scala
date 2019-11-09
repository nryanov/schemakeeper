package schemakeeper.server.api.internal

import schemakeeper.schema.CompatibilityType

case class SubjectSettings(compatibilityType: CompatibilityType, isLocked: Boolean)
