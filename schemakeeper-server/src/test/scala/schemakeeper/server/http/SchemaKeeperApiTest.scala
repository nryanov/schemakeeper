package schemakeeper.server.http

import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import org.junit.runner.RunWith
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach, OptionValues}
import org.scalatestplus.junit.JUnitRunner
import schemakeeper.schema.{CompatibilityType, SchemaType}
import schemakeeper.server.DBSpec
import schemakeeper.server.service._
import io.circe.syntax._
import org.apache.avro.Schema
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s._
import schemakeeper.api._
import schemakeeper.server.SchemaKeeperError._
import schemakeeper.server.http.internal.SubjectSettings
import schemakeeper.server.http.protocol.{ErrorCode, ErrorInfo}
import schemakeeper.server.http.protocol.JsonProtocol._

import scala.concurrent.ExecutionContext

@RunWith(classOf[JUnitRunner])
class SchemaKeeperApiTest extends DBSpec with OptionValues with BeforeAndAfterEach with BeforeAndAfterAll {
  implicit val ctx = IO.contextShift(ExecutionContext.global)

  var service: DBBackedService[F] = {
    val map: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]
    map.put("schemakeeper.storage.username", "")
    map.put("schemakeeper.storage.password", "")
    map.put("schemakeeper.storage.schema", "schemakeeper")
    map.put("schemakeeper.storage.driver", "org.h2.Driver")
    map.put("schemakeeper.storage.maxConnections", "1")
    map.put("schemakeeper.storage.url", "jdbc:h2:mem:schemakeeper;DB_CLOSE_DELAY=-1")

    val config: Config = ConfigFactory.parseMap(map)
    createService(config)
  }

  val api = SchemaKeeperApi.create[IO](service).route

  implicit val listOfStringsDecoder: EntityDecoder[IO, List[String]] = jsonOf[IO, List[String]]
  implicit val listOfIntDecoder: EntityDecoder[IO, List[Int]] = jsonOf[IO, List[Int]]
  implicit val errorInfoEntityDecoder: EntityDecoder[IO, ErrorInfo] = jsonOf[IO, ErrorInfo]
  implicit val subjectMetadataEntityDecoder: EntityDecoder[IO, SubjectMetadata] = jsonOf[IO, SubjectMetadata]
  implicit val listOfSubjectSchemaMetadataEntityDecoder: EntityDecoder[IO, List[SubjectSchemaMetadata]] =
    jsonOf[IO, List[SubjectSchemaMetadata]]
  implicit val subjectSettingsEntityDecoder: EntityDecoder[IO, SubjectSettings] = jsonOf[IO, SubjectSettings]
  implicit val schemaTextEntityDecoder: EntityDecoder[IO, SchemaText] = jsonOf[IO, SchemaText]
  implicit val schemaMetadataEntityDecoder: EntityDecoder[IO, SchemaMetadata] = jsonOf[IO, SchemaMetadata]
  implicit val schemaIdEntityDecoder: EntityDecoder[IO, SchemaId] = jsonOf[IO, SchemaId]
  implicit val booleanEntityDecoder: EntityDecoder[IO, Boolean] = jsonOf[IO, Boolean]

  def checkPredicate[A](actualResp: Response[IO], expectedStatus: Status, predicate: A => Boolean)(
    implicit ev: EntityDecoder[IO, A]
  ): Assertion = {
    assertResult(expectedStatus)(actualResp.status)
    assert(predicate(actualResp.as[A].unsafeRunSync))
  }

  def check[A](actualResp: Response[IO], expectedStatus: Status, expectedBody: A)(
    implicit ev: EntityDecoder[IO, A]
  ): Assertion = {
    assertResult(expectedStatus)(actualResp.status)
    assertResult(expectedBody)(actualResp.as[A].unsafeRunSync)
  }

  def runRequest(request: Request[IO]): IO[Response[IO]] = api.run(request).value.map(_.value)

  "Subject endpoint" should {
    "return subject list" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[List[String]](response, Status.Ok, List("A1"))
      }
    }

    "return empty subject list" in runF {
      val request: Request[IO] = Request[IO](method = Method.GET, uri = uri"/v2/subjects")

      for {
        response <- runRequest(request)
      } yield {
        checkPredicate[List[String]](response, Status.Ok, _.isEmpty)
      }
    }
  }

  "SubjectMetadata endpoint" should {
    "return subject metadata" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[SubjectMetadata](response, Status.Ok, SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))
      }
    }

    "NotFound - subject does not exist" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1")

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }
  }

  "UpdateSubjectSettings endpoint" should {
    "return updated subject metadata" in runF {
      val body = SubjectSettings(CompatibilityType.FORWARD, isLocked = true)
      val request = Request[IO](method = Method.PUT, uri = uri"/v2/subjects/A1").withEntity(body.asJson)

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[SubjectMetadata](
          response,
          Status.Ok,
          SubjectMetadata.instance("A1", CompatibilityType.FORWARD, true)
        )
      }
    }

    "NotFound - subject does not exist" in runF {
      val body = SubjectSettings(CompatibilityType.FORWARD, isLocked = true)
      val request = Request[IO](method = Method.PUT, uri = uri"/v2/subjects/A1").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }
  }

  "SubjectVersions endpoint" should {
    "return versions list" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions")

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        check[List[Int]](response, Status.Ok, List(1))

      }
    }

    "return empty versions list" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        checkPredicate[List[Int]](response, Status.Ok, _.isEmpty)

      }
    }

    "NotFound - subject does not exist" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions")

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }
  }

  "SubjectSchemasMetadata endpoint" should {
    "return meta list" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/schemas")

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        checkPredicate[List[SubjectSchemaMetadata]](response, Status.Ok, _.size == 1)
      }
    }

    "return empty list" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/schemas")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(SubjectHasNoRegisteredSchemas("A1").msg, ErrorCode.SubjectHasNoRegisteredSchemasCode)
        )
      }
    }

    "NotFound - subject does not exist" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/schemas")

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }
  }

  "SubjectSchemaByVersion endpoint" should {
    "return meta" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions/1")

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        checkPredicate[SchemaText](
          response,
          Status.Ok,
          _.getSchemaText == Schema.create(Schema.Type.STRING).toString
        )
      }
    }

    "NotFound - subject does not exist" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions/1")

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }

    "NotFound - subject has no schema with such version" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions/2")

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(
            SubjectSchemaVersionDoesNotExist("A1", 2).msg,
            ErrorCode.SubjectSchemaVersionDoesNotExistCode
          )
        )

      }
    }

    "throws validation error" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions/-1")

      for {
        response <- runRequest(request)
      } yield {
        check[String](
          response,
          Status.BadRequest,
          "Invalid value for: path parameter ? (expected value to be greater than or equal to 1, but was -1)"
        )
      }
    }
  }

  "SchemaById endpoint" should {
    "return meta" in runF {
      def request(id: Int) = Request[IO](method = Method.GET, uri = uri"/v2/schemas/$id")

      for {
        id <- service.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        response <- runRequest(request(id.getSchemaId))
      } yield {
        checkPredicate[SchemaMetadata](
          response,
          Status.Ok,
          _.getSchemaId == id.getSchemaId
        )
      }
    }

    "NotFound - schema with specified id does not exist" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/schemas/123")

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SchemaIdDoesNotExist(1).msg, ErrorCode.SchemaIdDoesNotExistCode)
        )
      }
    }

    "throws validation error" in runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/schemas/-1")

      for {
        response <- runRequest(request)
      } yield {
        check[String](
          response,
          Status.BadRequest,
          "Invalid value for: path parameter ? (expected value to be greater than or equal to 1, but was -1)"
        )
      }
    }
  }

  "SchemaIdBySubjectAndSchema endpoint" should {
    "return SchemaId" in runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING))
      val request = Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas/id").withEntity(body.asJson)

      for {
        id <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        check[SchemaId](
          response,
          Status.Ok,
          SchemaId.instance(id.getSchemaId)
        )
      }
    }

    "NotFound - schema is not registered" in runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING))
      val request = Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas/id").withEntity(body.asJson)

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(
            SchemaIsNotRegistered(Schema.create(Schema.Type.STRING).toString()).msg,
            ErrorCode.SchemaIsNotRegisteredCode
          )
        )
      }
    }

    "BadRequest - schema is not valid" in {
      val body = SchemaText.instance("not valid schema")
      val request = Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas/id").withEntity(body.asJson)

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode)
        )
      }
    }

    "BadRequest - schema is not connected to subject" in runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING))
      val request = Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas/id").withEntity(body.asJson)

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        id <- service.registerSchema(Schema.create(Schema.Type.STRING).toString(), SchemaType.AVRO)
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(
            SubjectIsNotConnectedToSchema("A1", id.getSchemaId).msg,
            ErrorCode.SubjectIsNotConnectedToSchemaCode
          )
        )
      }
    }
  }

  "DeleteSubject endpoint" should {
    "return true" in runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        checkPredicate[Boolean](response, Status.Ok, r => r)
      }
    }

    "return false" in runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1")

      for {
        response <- runRequest(request)
      } yield {
        checkPredicate[Boolean](response, Status.Ok, r => !r)
      }
    }
  }

  "DeleteSubjectSchemaByVersion endpoint" should {
    "return true" in runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1/versions/1")

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        checkPredicate[Boolean](response, Status.Ok, r => r)
      }
    }

    "BadRequest - subject does not exist" in runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1/versions/1")

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }

    "BadRequest - specified version does not exist" in runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1/versions/123")

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(
            SubjectSchemaVersionDoesNotExist("A1", 123).msg,
            ErrorCode.SubjectSchemaVersionDoesNotExistCode
          )
        )
      }
    }

    "throws validation error" in runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1/versions/-1")

      for {
        response <- runRequest(request)
      } yield {
        check[String](
          response,
          Status.BadRequest,
          "Invalid value for: path parameter ? (expected value to be greater than or equal to 1, but was -1)"
        )
      }
    }
  }

  "CheckSubjectCompatibility endpoint" should {
    "return true" in runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.INT).toString)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/compatibility/schemas").withEntity(body.asJson)

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.NONE,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        checkPredicate[Boolean](response, Status.Ok, r => r)
      }
    }

    "return false" in runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.INT).toString)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/compatibility/schemas").withEntity(body.asJson)

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        checkPredicate[Boolean](response, Status.Ok, r => !r)
      }
    }

    "BadRequest - schema is not a valid avro schema" in runF {
      val body = SchemaText.instance("not valid schema")
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/compatibility/schemas").withEntity(body.asJson)

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.NONE,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode)
        )
      }
    }

    "BadRequest - subject does not exist" in runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.INT).toString)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/compatibility/schemas").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }
  }

  "RegisterSchema endpoint" should {
    "return schemaId" in runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING).toString)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/schemas").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        checkPredicate[SchemaId](response, Status.Ok, _ => true)
      }
    }

    "BadRequest - schema is not valid" in runF {
      val body = SchemaText.instance("not valid schema")
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/schemas").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode)
        )
      }
    }

    "BadRequest - schema is already exist" in {
      val body = SchemaText.instance("not valid schema")
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/schemas").withEntity(body.asJson)

      for {
        schema <- service.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(
            SchemaIsAlreadyExist(schema.getSchemaId, Schema.create(Schema.Type.STRING).toString()).msg,
            ErrorCode.SchemaIsAlreadyExistCode
          )
        )
      }
    }
  }

//  "RegisterSchemaAndSubject endpoint" should {
//    "return schemaId" in {
//      val api = SchemaKeeperApi(service)
//      val body = SubjectAndSchemaRequest.instance(
//        Schema.create(Schema.Type.STRING).toString,
//        SchemaType.AVRO,
//        CompatibilityType.BACKWARD
//      )
//      val result = api
//        .registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body))
//        .awaitValueUnsafe()
//      assert(result.get.isInstanceOf[SchemaId])
//    }
//
//    "return schemaId - schema and subject are already registered and connected" in {
//      val id = service
//        .registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
//        .unsafeRunSync()
//        .right
//        .get
//      val api = SchemaKeeperApi(service)
//      val body = SubjectAndSchemaRequest.instance(
//        Schema.create(Schema.Type.STRING).toString,
//        SchemaType.AVRO,
//        CompatibilityType.BACKWARD
//      )
//      val result = api
//        .registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body))
//        .awaitValueUnsafe()
//      assertResult(id)(result.get)
//    }
//
//    "BadRequest - schema is not valid" in {
//      val api = SchemaKeeperApi(service)
//      val body = SubjectAndSchemaRequest.instance("not valid schema", SchemaType.AVRO, CompatibilityType.BACKWARD)
//      val result = api
//        .registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body))
//        .awaitOutputUnsafe()
//      assertResult(
//        Output.failure(
//          ErrorInfo(SchemaIsNotValid("not valid schema").msg, ErrorCode.SchemaIsNotValidCode),
//          Status.BadRequest
//        )
//      )(result.get)
//    }
//
//    "BadRequest - subject is locked" in {
//      val api = SchemaKeeperApi(service)
//      service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true).unsafeRunSync()
//      val body = SubjectAndSchemaRequest.instance(
//        Schema.create(Schema.Type.INT).toString,
//        SchemaType.AVRO,
//        CompatibilityType.BACKWARD
//      )
//      val result = api
//        .registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body))
//        .awaitOutputUnsafe()
//      assertResult(
//        Output.failure(ErrorInfo(SubjectIsLocked("A1").msg, ErrorCode.SubjectIsLockedErrorCode), Status.BadRequest)
//      )(result.get)
//    }
//
//    "BadRequest - schema is not compatible" in {
//      service
//        .registerSchema("A1", Schema.create(Schema.Type.INT).toString, CompatibilityType.BACKWARD, SchemaType.AVRO)
//        .unsafeRunSync()
//      val api = SchemaKeeperApi(service)
//      val body = SubjectAndSchemaRequest.instance(
//        Schema.create(Schema.Type.STRING).toString,
//        SchemaType.AVRO,
//        CompatibilityType.BACKWARD
//      )
//      val result = api
//        .registerSchemaAndSubject(Input.post("/v2/subjects/A1/schemas").withBody[Application.Json](body))
//        .awaitOutputUnsafe()
//      assertResult(
//        Output.failure(
//          ErrorInfo(
//            service
//              .SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD)
//              .msg,
//            ErrorCode.SchemaIsNotCompatibleCode
//          ),
//          Status.BadRequest
//        )
//      )(result.get)
//    }
//  }
//
//  "RegisterSubject endpoint" should {
//    "return ok" in {
//      val api = SchemaKeeperApi(service)
//      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD)
//      val result = api.registerSubject(Input.post("/v2/subjects").withBody[Application.Json](body)).awaitOutputUnsafe()
//      assertResult(Ok(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD)))(result.get)
//    }
//
//    "return ok - register locked subject" in {
//      val api = SchemaKeeperApi(service)
//      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true)
//      val result = api.registerSubject(Input.post("/v2/subjects").withBody[Application.Json](body)).awaitOutputUnsafe()
//      assertResult(Ok(SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true)))(result.get)
//    }
//
//    "BadRequest - subject is already exist" in {
//      service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
//      val api = SchemaKeeperApi(service)
//      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD)
//      val result = api.registerSubject(Input.post("/v2/subjects").withBody[Application.Json](body)).awaitOutputUnsafe()
//      assertResult(
//        Output
//          .failure(ErrorInfo(SubjectIsAlreadyExists("A1").msg, ErrorCode.SubjectIsAlreadyExistsCode), Status.BadRequest)
//      )(result.get)
//    }
//  }
//
//  "AddSchemaToSubject endpoint" should {
//    "return version number - first schema" in {
//      service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
//      val id = service
//        .registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
//        .unsafeRunSync()
//        .right
//        .get
//        .getSchemaId
//      val api = SchemaKeeperApi(service)
//      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitValueUnsafe()
//      assert(result.contains(1))
//    }
//
//    "return version number - second schema" in {
//      service
//        .registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.NONE, SchemaType.AVRO)
//        .unsafeRunSync()
//      val id = service
//        .registerSchema(Schema.create(Schema.Type.INT).toString, SchemaType.AVRO)
//        .unsafeRunSync()
//        .right
//        .get
//        .getSchemaId
//      val api = SchemaKeeperApi(service)
//      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitValueUnsafe()
//      assert(result.contains(2))
//    }
//
//    "BadRequest - subject and schema are already connected" in {
//      val id = service
//        .registerSchema("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.NONE, SchemaType.AVRO)
//        .unsafeRunSync()
//        .right
//        .get
//        .getSchemaId
//      val api = SchemaKeeperApi(service)
//      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitOutputUnsafe()
//      assertResult(
//        Output.failure(
//          ErrorInfo(SubjectIsAlreadyConnectedToSchema("A1", id).msg, ErrorCode.SubjectIsAlreadyConnectedToSchemaCode),
//          Status.BadRequest
//        )
//      )(result.get)
//    }
//
//    "BadRequest - subject is locked" in {
//      val api = SchemaKeeperApi(service)
//      service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true).unsafeRunSync()
//      val id = service
//        .registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
//        .unsafeRunSync()
//        .right
//        .get
//        .getSchemaId
//      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitOutputUnsafe()
//      assertResult(
//        Output.failure(ErrorInfo(SubjectIsLocked("A1").msg, ErrorCode.SubjectIsLockedErrorCode), Status.BadRequest)
//      )(result.get)
//    }
//
//    "NotFound - subject does not exist" in {
//      val id = service
//        .registerSchema(Schema.create(Schema.Type.INT).toString, SchemaType.AVRO)
//        .unsafeRunSync()
//        .right
//        .get
//        .getSchemaId
//      val api = SchemaKeeperApi(service)
//      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/$id")).awaitOutputUnsafe()
//      assertResult(
//        Output.failure(ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode), Status.NotFound)
//      )(result.get)
//    }
//
//    "NotFound - schema does not exist" in {
//      service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false).unsafeRunSync()
//      val api = SchemaKeeperApi(service)
//      val result = api.addSchemaToSubject(Input.post(s"/v2/subjects/A1/schemas/123")).awaitOutputUnsafe()
//      assertResult(
//        Output.failure(ErrorInfo(SchemaIdDoesNotExist(123).msg, ErrorCode.SchemaIdDoesNotExistCode), Status.NotFound)
//      )(result.get)
//    }
//
//    "throws validation error" in {
//      val api = SchemaKeeperApi(service)
//      assertThrows[NotValid](api.addSchemaToSubject(Input.post("/v2/subjects/A1/schemas/-1")).awaitOutputUnsafe())
//    }
//  }
}
