package schemakeeper.server

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfigurationSpec extends IOSpec {
  "configuration" should {
    "load config" in runF {
      val expected = Configuration(
        Storage(
          url = "url",
          driver = "driver",
          username = "username",
          schema = "schema"
        ),
        Server(port = 12345)
      )

      for {
        cfg <- Configuration.create[F]("""
            |schemakeeper {
            |storage {
            |url = url
            |driver = driver
            |username = username
            |schema = schema
            |}
            |
            |server {
            |port = 12345
            |}
            |}
            |""".stripMargin)
      } yield {
        assertResult(expected)(cfg)
      }
    }
  }
}
