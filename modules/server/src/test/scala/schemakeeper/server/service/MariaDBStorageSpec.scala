//package schemakeeper.server.service
//
//import java.sql.{Connection, DriverManager}
//import java.util
//
//import com.dimafeng.testcontainers.{ContainerDef, JdbcDatabaseContainer, SingleContainer}
//import com.dimafeng.testcontainers.scalatest.TestContainerForAll
//import com.typesafe.config.{Config, ConfigFactory}
//import org.junit.runner.RunWith
//import org.scalatestplus.junit.JUnitRunner
//import schemakeeper.server.DBSpec
// // todo
//@RunWith(classOf[JUnitRunner])
//class MariaDBStorageSpec extends ServiceSpec with TestContainerForAll with DBSpec {
//  override val containerDef: MariaDBStorageSpec.MariaDBContainer.Def =
//    MariaDBStorageSpec.MariaDBContainer.Def(dbName = "schemakeeper")
//
//  override var schemaStorage: DBBackedService[F] = _
//
//  override def afterContainersStart(container: MariaDBStorageSpec.MariaDBContainer): Unit = {
//    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
//    map.put("schemakeeper.storage.username", container.username)
//    map.put("schemakeeper.storage.password", container.password)
//    map.put("schemakeeper.storage.schema", "schemakeeper")
//    map.put("schemakeeper.storage.driver", container.driverClassName)
//    map.put("schemakeeper.storage.url", container.jdbcUrl)
//
//    val config: Config = ConfigFactory.parseMap(map)
//    schemaStorage = createService(config)
//  }
//}
//
//object MariaDBStorageSpec {
//  import org.testcontainers.containers.{MariaDBContainer => JavaMariaDBContainer}
//
//  case class MariaDBContainer(
//    dockerImageName: String = MariaDBContainer.defaultDockerImageName,
//    dbName: String = MariaDBContainer.defaultDatabaseName,
//    dbUsername: String = MariaDBContainer.defaultUsername,
//    dbPassword: String = MariaDBContainer.defaultPassword,
//    configurationOverride: Option[String] = None
//  ) extends SingleContainer[JavaMariaDBContainer[_]]
//      with JdbcDatabaseContainer {
//
//    override val container: JavaMariaDBContainer[_] = {
//      val c = new JavaMariaDBContainer(dockerImageName)
//      c.withDatabaseName(dbName)
//      c.withUsername(dbUsername)
//      c.withPassword(dbPassword)
//      configurationOverride.foreach(c.withConfigurationOverride)
//      c
//    }
//
//    def testQueryString: String = container.getTestQueryString
//
//  }
//
//  object MariaDBContainer {
//
//    val defaultDockerImageName = s"${JavaMariaDBContainer.IMAGE}:${JavaMariaDBContainer.DEFAULT_TAG}"
//    val defaultDatabaseName = "test"
//    val defaultUsername = "test"
//    val defaultPassword = "test"
//
//    case class Def(
//      dockerImageName: String = MariaDBContainer.defaultDockerImageName,
//      dbName: String = MariaDBContainer.defaultDatabaseName,
//      dbUsername: String = MariaDBContainer.defaultUsername,
//      dbPassword: String = MariaDBContainer.defaultPassword,
//      configurationOverride: Option[String] = None
//    ) extends ContainerDef {
//
//      override type Container = MariaDBContainer
//
//      override def createContainer(): MariaDBContainer =
//        new MariaDBContainer(
//          dockerImageName,
//          dbName,
//          dbUsername,
//          dbPassword,
//          configurationOverride
//        )
//    }
//
//  }
//
//}
