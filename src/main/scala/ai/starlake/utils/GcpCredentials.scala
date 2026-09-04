package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobBase.{
  getJsonKeyAbsoluteFile,
  getJsonKeyStream,
  getJsonKeyStreamFromBase64
}
import com.google.auth.Credentials
import com.google.auth.oauth2.{
  AccessToken,
  GoogleCredentials,
  ServiceAccountCredentials,
  UserCredentials
}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object GcpCredentials extends LazyLogging {
  def credentials(
    connectionOptions: Map[String, String],
    accessToken: scala.Option[String] = None
  )(implicit settings: Settings): scala.Option[Credentials] = {
    accessToken match {
      case Some(token) =>
        logger.info(s"Using inline access token credentials")
        val cred = GoogleCredentials.create(new AccessToken(token, null))
        scala.Option(cred)
      case None =>
        logger.info(s"Using ${connectionOptions("authType")} credentials")
        connectionOptions("authType") match {
          case "APPLICATION_DEFAULT" =>
            val refreshToken =
              Try(connectionOptions.getOrElse("refreshToken", "true").toBoolean).getOrElse(true)
            if (refreshToken) {
              val scopes = connectionOptions
                .getOrElse("authScopes", "https://www.googleapis.com/auth/cloud-platform")
                .split(',')
              val cred = GoogleCredentials.getApplicationDefault().createScoped(scopes: _*)
              Try {
                cred.refresh()
              } match {
                case Failure(e) =>
                  logger.error(s"Error refreshing credentials: ${e.getMessage}", e)
                  None
                case Success(_) =>
                  Some(cred)
              }
            } else {
              scala.Option(GoogleCredentials.getApplicationDefault())
            }
          case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
            val credentialsStream = getJsonKeyStream(connectionOptions)
            scala.Option(ServiceAccountCredentials.fromStream(credentialsStream))
          case "SERVICE_ACCOUNT_JSON_KEY_BASE64" =>
            val base64Key = getJsonKeyStreamFromBase64(connectionOptions)
            scala.Option(ServiceAccountCredentials.fromStream(base64Key))

          case "USER_CREDENTIALS" =>
            val clientId = connectionOptions("clientId")
            val clientSecret = connectionOptions("clientSecret")
            val refreshToken = connectionOptions("refreshToken")
            val cred = UserCredentials
              .newBuilder()
              .setClientId(clientId)
              .setClientSecret(clientSecret)
              .setRefreshToken(refreshToken)
              .build()
            scala.Option(cred)

          case "ACCESS_TOKEN" =>
            val accessToken = connectionOptions("gcpAccessToken")
            val cred = GoogleCredentials.create(new AccessToken(accessToken, null))
            scala.Option(cred)
        }
    }
  }

  /** Maps a connection's `authType` (as declared in the YAML config) onto the google.cloud.auth.*
    * Hadoop properties the GCS connector reads.
    *
    * gcs-connector defaults `google.cloud.auth.type` to COMPUTE_ENGINE, so any Hadoop configuration
    * that does not carry these keys ends up querying the GCE metadata server, which only exists on
    * Google infrastructure.
    */
  def hadoopAuthConf(
    connectionOptions: Map[String, String]
  )(implicit settings: Settings): Map[String, String] = {
    val authType = connectionOptions.getOrElse("authType", "APPLICATION_DEFAULT")
    val authConf = authType match {
      case "APPLICATION_DEFAULT" =>
        Map(
          "google.cloud.auth.type" -> "APPLICATION_DEFAULT"
        )
      /*
          val gcpAccessToken =
            GcpUtils.getCredentialUsingWellKnownFile().asInstanceOf[UserCredentials]

          Map(
            "google.cloud.auth.type"                   -> "USER_CREDENTIALS",
            "google.cloud.auth.service.account.enable" -> "true",
            "google.cloud.auth.client.id"              -> gcpAccessToken.getClientId,
            "google.cloud.auth.client.secret"          -> gcpAccessToken.getClientSecret,
            "google.cloud.auth.refresh.token"          -> gcpAccessToken.getRefreshToken
          )

       */
      case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
        val jsonKeyFilename = connectionOptions.getOrElse(
          "jsonKeyfile",
          throw new Exception("jsonKeyfile attribute is required for SERVICE_ACCOUNT_JSON_KEYFILE")
        )

        val jsonKeyFile = getJsonKeyAbsoluteFile(jsonKeyFilename)
        if (!jsonKeyFile.exists()) {
          throw new Exception(s"jsonKeyfile $jsonKeyFilename does not exist")
        }

        Map(
          "google.cloud.auth.type"                         -> "SERVICE_ACCOUNT_JSON_KEYFILE",
          "google.cloud.auth.service.account.enable"       -> "true",
          "google.cloud.auth.service.account.json.keyfile" -> jsonKeyFile.toString()
        )
      case "USER_CREDENTIALS" =>
        val clientId = connectionOptions.getOrElse(
          "clientId",
          throw new Exception("clientId attribute is required for USER_CREDENTIALS")
        )
        val clientSecret = connectionOptions.getOrElse(
          "clientSecret",
          throw new Exception("clientSecret attribute is required for USER_CREDENTIALS")
        )
        val refreshToken = connectionOptions.getOrElse(
          "refreshToken",
          throw new Exception("refreshToken attribute is required for USER_CREDENTIALS")
        )
        Map(
          "google.cloud.auth.type"                   -> "USER_CREDENTIALS",
          "google.cloud.auth.service.account.enable" -> "true",
          "google.cloud.auth.client.id"              -> clientId,
          "google.cloud.auth.client.secret"          -> clientSecret,
          "google.cloud.auth.refresh.token"          -> refreshToken
        )
      case _ =>
        Map.empty[String, String]
    }
    authConf
  }
}
