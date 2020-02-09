package schemakeeper.server.api.internal

import schemakeeper.schema.CompatibilityType

final case class SubjectSettings(compatibilityType: CompatibilityType, isLocked: Boolean)
