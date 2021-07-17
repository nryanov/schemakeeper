package schemakeeper.server.http.internal

import schemakeeper.schema.CompatibilityType

final case class SubjectSettings(compatibilityType: CompatibilityType, isLocked: Boolean)
