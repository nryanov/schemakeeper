package schemakeeper.server.http

import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
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

class SchemaKeeperApiTest extends DBSpec {
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
  implicit val intEntityDecoder: EntityDecoder[IO, Int] = jsonOf[IO, Int]

  def checkPredicate[A](actualResp: Response[IO], expectedStatus: Status, predicate: A => Boolean)(
    implicit ev: EntityDecoder[IO, A]
  ) = {
    assertEquals(expectedStatus, actualResp.status)
    assert(predicate(actualResp.as[A].unsafeRunSync))
  }

  def check[A](actualResp: Response[IO], expectedStatus: Status, expectedBody: A)(
    implicit ev: EntityDecoder[IO, A]
  ) = {
    assertEquals(expectedStatus, actualResp.status)
    assertEquals(expectedBody, actualResp.as[A].unsafeRunSync)
  }

  def runRequest(request: Request[IO]): IO[Response[IO]] =
    api.run(request).value.map(_.getOrElse(throw new IllegalStateException("None")))

  test("Subject endpoint should return subject list") {
    runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[List[String]](response, Status.Ok, List("A1"))
      }
    }
  }

  test("Subject endpoint should return empty subject list") {
    runF {
      val request: Request[IO] = Request[IO](method = Method.GET, uri = uri"/v2/subjects")

      for {
        response <- runRequest(request)
      } yield {
        checkPredicate[List[String]](response, Status.Ok, _.isEmpty)
      }
    }
  }

  test("SubjectMetadata endpoint should return subject metadata") {
    runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[SubjectMetadata](response, Status.Ok, SubjectMetadata.instance("A1", CompatibilityType.BACKWARD))
      }
    }
  }

  test("SubjectMetadata endpoint should return NotFound - subject does not exist") {
    runF {
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

  test("UpdateSubjectSettings endpoint should return updated subject metadata") {
    runF {
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
  }

  test("UpdateSubjectSettings endpoint should return NotFound - subject does not exist") {
    runF {
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

  test("SubjectVersions endpoint should return versions list") {
    runF {
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
  }

  test("SubjectVersions endpoint should return empty versions list") {
    runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/subjects/A1/versions")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        checkPredicate[List[Int]](response, Status.Ok, _.isEmpty)

      }
    }
  }

  test("SubjectVersions endpoint should return NotFound - subject does not exist") {
    runF {
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

  test("SubjectSchemasMetadata endpoint should return meta list") {
    runF {
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
  }

  test("SubjectSchemasMetadata endpoint should return empty list") {
    runF {
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
  }

  test("SubjectSchemasMetadata endpoint should  return NotFound - subject does not exist") {
    runF {
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

  test("SubjectSchemaByVersion endpoint should return meta") {
    runF {
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
  }

  test("SubjectSchemaByVersion endpoint should return NotFound - subject does not exist") {
    runF {
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
  }

  test("SubjectSchemaByVersion endpoint should  return NotFound - subject has no schema with such version") {
    runF {
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
  }

  test("SubjectSchemaByVersion endpoint should throws validation error") {
    runF {
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

  test("SchemaById endpoint should return meta") {
    runF {
      def request(id: Int) = Request[IO](method = Method.GET, uri = Uri(path = s"/v2/schemas/$id"))

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
  }

  test("SchemaById endpoint should return NotFound - schema with specified id does not exist") {
    runF {
      val request = Request[IO](method = Method.GET, uri = uri"/v2/schemas/123")

      for {
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SchemaIdDoesNotExist(123).msg, ErrorCode.SchemaIdDoesNotExistCode)
        )
      }
    }
  }

  test("SchemaById endpoint should throws validation error") {
    runF {
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

  test("SchemaIdBySubjectAndSchema endpoint should return SchemaId") {
    runF {
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
  }

  test("SchemaIdBySubjectAndSchema endpoint should  return NotFound - schema is not registered") {
    runF {
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
  }

  test("SchemaIdBySubjectAndSchema endpoint should return BadRequest - schema is not valid") {
    runF {
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
  }

  test("SchemaIdBySubjectAndSchema endpoint should reuturn BadRequest - schema is not connected to subject") {
    runF {
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

  test("DeleteSubject endpoint should return true") {
    runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1")

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        checkPredicate[Boolean](response, Status.Ok, r => r)
      }
    }
  }

  test("DeleteSubject endpoint should  should return false") {
    runF {
      val request = Request[IO](method = Method.DELETE, uri = uri"/v2/subjects/A1")

      for {
        response <- runRequest(request)
      } yield {
        checkPredicate[Boolean](response, Status.Ok, r => !r)
      }
    }
  }

  test("DeleteSubjectSchemaByVersion endpoint should return true") {
    runF {
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
  }

  test("DeleteSubjectSchemaByVersion endpoint should return BadRequest - subject does not exist") {
    runF {
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
  }

  test("DeleteSubjectSchemaByVersion endpoint should return BadRequest - specified version does not exist") {
    runF {
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
  }

  test("DeleteSubjectSchemaByVersion endpoint should throws validation error") {
    runF {
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

  test("CheckSubjectCompatibility endpoint should return true") {
    runF {
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
  }

  test("CheckSubjectCompatibility endpoint should return false") {
    runF {
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
  }

  test("CheckSubjectCompatibility endpoint should return BadRequest - schema is not a valid avro schema") {
    runF {
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
  }

  test("CheckSubjectCompatibility endpoint should return BadRequest - subject does not exist") {
    runF {
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

  test("RegisterSchema endpoint should return schemaId") {
    runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING).toString)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/schemas").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        checkPredicate[SchemaId](response, Status.Ok, _ => true)
      }
    }
  }

  test("RegisterSchema endpoint should return BadRequest - schema is not valid") {
    runF {
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
  }

  test("RegisterSchema endpoint should return BadRequest - schema is already exist") {
    runF {
      val body = SchemaText.instance(Schema.create(Schema.Type.STRING).toString)
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

  test("RegisterSchemaAndSubject endpoint should return schemaId") {
    runF {
      val body = SubjectAndSchemaRequest.instance(
        Schema.create(Schema.Type.STRING).toString,
        SchemaType.AVRO,
        CompatibilityType.BACKWARD
      )
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        checkPredicate[SchemaId](response, Status.Ok, _ => true)
      }
    }
  }

  test(
    "RegisterSchemaAndSubject endpoint should return schemaId - schema and subject are already registered and connected"
  ) {
    runF {
      val body = SubjectAndSchemaRequest.instance(
        Schema.create(Schema.Type.STRING).toString,
        SchemaType.AVRO,
        CompatibilityType.BACKWARD
      )
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas").withEntity(body.asJson)

      for {
        id <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        checkPredicate[SchemaId](response, Status.Ok, _ == id)
      }
    }
  }

  test("RegisterSchemaAndSubject endpoint should return BadRequest - schema is not valid") {
    runF {
      val body = SubjectAndSchemaRequest.instance("not valid schema", SchemaType.AVRO, CompatibilityType.BACKWARD)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas").withEntity(body.asJson)

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
  }

  test("RegisterSchemaAndSubject endpoint should return BadRequest - subject is locked") {
    runF {
      val body = SubjectAndSchemaRequest.instance(
        Schema.create(Schema.Type.INT).toString,
        SchemaType.AVRO,
        CompatibilityType.BACKWARD
      )
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas").withEntity(body.asJson)

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(SubjectIsLocked("A1").msg, ErrorCode.SubjectIsLockedErrorCode)
        )
      }
    }
  }

  test("RegisterSchemaAndSubject endpoint should return BadRequest - schema is not compatible") {
    runF {
      val body = SubjectAndSchemaRequest.instance(
        Schema.create(Schema.Type.STRING).toString,
        SchemaType.AVRO,
        CompatibilityType.BACKWARD
      )
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas").withEntity(body.asJson)

      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.INT).toString,
          CompatibilityType.BACKWARD,
          SchemaType.AVRO
        )
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(
            SchemaIsNotCompatible("A1", Schema.create(Schema.Type.STRING).toString, CompatibilityType.BACKWARD).msg,
            ErrorCode.SchemaIsNotCompatibleCode
          )
        )
      }
    }
  }

  test("RegisterSubject endpoint should return ok") {
    runF {
      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        check[SubjectMetadata](response, Status.Ok, body)
      }
    }
  }

  test("RegisterSubject endpoint should return ok - register locked subject") {
    runF {
      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, true)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects").withEntity(body.asJson)

      for {
        response <- runRequest(request)
      } yield {
        check[SubjectMetadata](response, Status.Ok, body)
      }
    }
  }

  test("RegisterSubject endpoint should return BadRequest - subject is already exist") {
    runF {
      val body = SubjectMetadata.instance("A1", CompatibilityType.BACKWARD, false)
      val request =
        Request[IO](method = Method.POST, uri = uri"/v2/subjects").withEntity(body.asJson)

      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(request)
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(SubjectIsAlreadyExists("A1").msg, ErrorCode.SubjectIsAlreadyExistsCode)
        )
      }
    }
  }

  test("AddSchemaToSubject endpoint should return version number - first schema") {
    runF {
      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        id <- service.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        response <- runRequest(
          Request[IO](method = Method.POST, uri = Uri(path = s"/v2/subjects/A1/schemas/${id.getSchemaId}"))
        )
      } yield {
        check[Int](response, Status.Ok, 1)
      }
    }
  }

  test("AddSchemaToSubject endpoint should return version number - second schema") {
    runF {
      for {
        _ <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.NONE,
          SchemaType.AVRO
        )
        id <- service.registerSchema(Schema.create(Schema.Type.INT).toString, SchemaType.AVRO)
        response <- runRequest(
          Request[IO](method = Method.POST, uri = Uri(path = s"/v2/subjects/A1/schemas/${id.getSchemaId}"))
        )
      } yield {
        check[Int](response, Status.Ok, 2)
      }
    }
  }

  test("AddSchemaToSubject endpoint should return BadRequest - subject and schema are already connected") {
    runF {
      for {
        id <- service.registerSchema(
          "A1",
          Schema.create(Schema.Type.STRING).toString,
          CompatibilityType.NONE,
          SchemaType.AVRO
        )
        response <- runRequest(
          Request[IO](method = Method.POST, uri = Uri(path = s"/v2/subjects/A1/schemas/${id.getSchemaId}"))
        )
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(
            SubjectIsAlreadyConnectedToSchema("A1", id.getSchemaId).msg,
            ErrorCode.SubjectIsAlreadyConnectedToSchemaCode
          )
        )
      }
    }
  }

  test("AddSchemaToSubject endpoint should return BadRequest - subject is locked") {
    runF {
      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = true)
        id <- service.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        response <- runRequest(
          Request[IO](method = Method.POST, uri = Uri(path = s"/v2/subjects/A1/schemas/${id.getSchemaId}"))
        )
      } yield {
        check[ErrorInfo](
          response,
          Status.BadRequest,
          ErrorInfo(SubjectIsLocked("A1").msg, ErrorCode.SubjectIsLockedErrorCode)
        )
      }
    }
  }

  test("AddSchemaToSubject endpoint should return NotFound - subject does not exist") {
    runF {
      for {
        id <- service.registerSchema(Schema.create(Schema.Type.STRING).toString, SchemaType.AVRO)
        response <- runRequest(
          Request[IO](method = Method.POST, uri = Uri(path = s"/v2/subjects/A1/schemas/${id.getSchemaId}"))
        )
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SubjectDoesNotExist("A1").msg, ErrorCode.SubjectDoesNotExistCode)
        )
      }
    }
  }

  test("AddSchemaToSubject endpoint should return NotFound - schema does not exist") {
    runF {
      for {
        _ <- service.registerSubject("A1", CompatibilityType.BACKWARD, isLocked = false)
        response <- runRequest(Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas/123"))
      } yield {
        check[ErrorInfo](
          response,
          Status.NotFound,
          ErrorInfo(SchemaIdDoesNotExist(123).msg, ErrorCode.SchemaIdDoesNotExistCode)
        )
      }
    }
  }

  test("AddSchemaToSubject endpoint should throws validation error") {
    runF {
      for {
        response <- runRequest(Request[IO](method = Method.POST, uri = uri"/v2/subjects/A1/schemas/-1"))
      } yield {
        check[String](
          response,
          Status.BadRequest,
          "Invalid value for: path parameter ? (expected value to be greater than or equal to 1, but was -1)"
        )
      }
    }
  }
}
