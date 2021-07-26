package schemakeeper.server

class ConfigurationSpec extends IOSpec {

  test("load config") {
    val expected = Configuration(
      Storage(
        url = "url",
        driver = "driver",
        username = "username",
        schema = "schema"
      ),
      Server(port = 12345)
    )

    runF(for {
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
      assertEquals(expected, cfg)
    })
  }

}
