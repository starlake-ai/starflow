package ai.starlake.extract.spi

import ai.starlake.config.Settings
import ai.starlake.extract.ExtractSchemaConfig
import ai.starlake.extract.impl.openapi.OpenAPISchemaExtractor
import ai.starlake.extract.impl.restapi.RestAPISchemaExtractor
import ai.starlake.schema.model.ExtractSchemasInfo

/** Factory object for creating instances of SchemaExtractor based on provided configuration.
  */
object SchemaExtractorFactory {
  def getExtractor(
    extractSchemaCliConfig: ExtractSchemaConfig,
    extractSchemas: ExtractSchemasInfo
  )(implicit settings: Settings): SchemaExtractor = {
    val connection = resolveConnection(extractSchemaCliConfig, extractSchemas, settings)
    (extractSchemas.openAPI, extractSchemas.restAPI) match {
      case (Some(openAPIConfig), _) =>
        new OpenAPISchemaExtractor(
          openAPIConfig,
          connection,
          extractSchemas.sanitizeAttributeName,
          extractSchemaCliConfig.snakeCase
        )
      case (_, Some(restAPIConfig)) =>
        new RestAPISchemaExtractor(restAPIConfig)
      case _ =>
        throw new RuntimeException(
          "Could not create an instance of SchemaExtractor with the given information"
        )
    }
  }

  private def resolveConnection(
    extractSchemaCliConfig: ExtractSchemaConfig,
    extractSchemas: ExtractSchemasInfo,
    settings: Settings
  ) = {
    extractSchemaCliConfig.connectionRef
      .orElse(extractSchemas.connectionRef)
      .map(settings.appConfig.getConnection)
      .getOrElse(settings.appConfig.getDefaultConnection())
  }
}
