package schemakeeper.server.http

import cats.effect.{ContextShift, Sync}
import sttp.tapir.docs.openapi._
import sttp.tapir.openapi.OpenAPI
import sttp.tapir.openapi.circe.yaml._
import sttp.tapir.swagger.http4s.SwaggerHttp4s
import schemakeeper.server.http.SchemaKeeperApi.apiVersion

class SwaggerApi[F[_]: Sync: ContextShift](api: SchemaKeeperApi[F]) {
  private val openApiDocs: OpenAPI = OpenAPIDocsInterpreter().toOpenAPI(
    List(
      api.subjectsEndpoint,
      api.subjectMetadataEndpoint,
      api.updateSubjectSettingsEndpoint,
      api.subjectVersionsEndpoint,
      api.subjectSchemasMetadataEndpoint,
      api.subjectSchemaByVersionEndpoint,
      api.schemaByIdEndpoint,
      api.schemaIdBySubjectAndSchemaEndpoint,
      api.deleteSubjectEndpoint,
      api.deleteSubjectSchemaByVersionEndpoint,
      api.checkSubjectSchemaCompatibilityEndpoint,
      api.registerSchemaEndpoint,
      api.registerSchemaAndSubjectEndpoint,
      api.registerSubjectEndpoint,
      api.addSchemaToSubjectEndpoint
    ),
    "Schemakeeper API",
    apiVersion
  )

  val route = new SwaggerHttp4s(openApiDocs.toYaml).routes
}

object SwaggerApi {
  def create[F[_]: Sync: ContextShift](api: SchemaKeeperApi[F]): SwaggerApi[F] = new SwaggerApi(api)
}
