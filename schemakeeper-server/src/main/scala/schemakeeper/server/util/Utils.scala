package schemakeeper.server.util

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import schemakeeper.schema.CompatibilityType

object Utils {
  private val compatibilityTypeNames: Map[String, CompatibilityType] = CompatibilityType
    .values()
    .map(v => (v.name.toLowerCase, v))
    .toMap

  def toMD5Hex(value: String): String =
    MessageDigest.getInstance("MD5").digest(value.getBytes(StandardCharsets.UTF_8))
      .map(0xFF & _)
      .map("%02x".format(_))
      .foldLeft("")(_ + _)
      .take(32) // just to be sure

  def compatibilityTypeFromString(value: String): Option[CompatibilityType] =
    compatibilityTypeNames.get(value.toLowerCase)

  def compatibilityTypeFromStringUnsafe(value: String): CompatibilityType =
    compatibilityTypeNames(value.toLowerCase)
}
