package schemakeeper.server.util

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object Utils {
  def toMD5Hex(value: String): String =
    MessageDigest
      .getInstance("MD5")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(0xff & _)
      .map("%02x".format(_))
      .foldLeft("")(_ + _)
      .take(32) // just to be sure
}
