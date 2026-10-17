package ai.starlake.job.load

import ai.starlake.TestHelper
import ai.starlake.config.PrivacyLevels
import ai.starlake.job.ingest.{DummyIngestionJob, IngestionJob}
import ai.starlake.schema.model.{DomainInfo, SchemaInfo, TableAttribute, TransformInput}
import ai.starlake.utils.{Md5, Sha256}
import com.typesafe.config.{Config, ConfigFactory}

import java.util.regex.Pattern

class PrivacyIngestionJobSpec extends TestHelper {

  private def sha256(value: String): String = Sha256.crypt(value, Map.empty, Nil)
  private def md5(value: String): String = Md5.crypt(value, Map.empty, Nil)

  new WithSettings() {
    "Privacy" should "be applied to the loaded values of a DSV file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/privacy/_config.sl.yml",
        datasetDomainName = "privacy",
        sourceDatasetPathName = "/sample/privacy/customers-1.dsv"
      ) {
        try {
          cleanMetadata
          deliverSourceDomain()
          deliverSourceTable("/sample/privacy/customers.sl.yml")
          loadPending.isSuccess shouldBe true

          val customers = sparkSession
            .sql(
              "SELECT id, email, name, phone, city, nickname, fullname, code, score, amount, token FROM privacy.customers ORDER BY id"
            )
            .collect()
            .toList
          // Carl is rejected: APPROXLONG20 fails on a value that is not a number
          customers.map(_.getLong(0)) shouldBe List(1L, 2L)
          customers.map(row => (1 to 7).map(i => Option(row.get(i)).orNull).toList) shouldBe List(
            List(
              sha256("alice@example.com"),
              "",
              md5("0102030405"),
              // SQL privacy is unchanged
              "PARIS",
              "XXXXXXXXXX",
              "A.M.",
              // Trimmed before the privacy is applied
              sha256("secret")
            ),
            // An empty value is null, and stays null. INITIALS ignores leading spaces
            List(sha256("bob@example.com"), "", null, "LYON", "XXXXXXXXXX", "B.S.", sha256("other"))
          )
          // HIDE on a long is null
          customers.map(row => Option(row.get(8))) shouldBe List(None, None)
          // APPROXLONG20 keeps the type and moves the value by 20% at most
          customers.map(_.getLong(9)).zip(List(1000L, 2000L)).foreach { case (amount, value) =>
            amount should (be >= (value * 0.8).toLong and be <= (value * 1.2).toLong)
          }
          customers.map(_.getString(10).toLong).zip(List(10L, 20L)).foreach { case (token, value) =>
            token should (be >= (value * 0.8).toLong and be <= (value * 1.2).toLong)
          }
        } finally {
          sparkSession.sql("DROP TABLE IF EXISTS privacy.customers")
        }
      }
    }

    it should "be applied to nested fields, array elements and variants of a JSON file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/privacy/_config.sl.yml",
        datasetDomainName = "privacy",
        sourceDatasetPathName = "/sample/privacy/orders-1.json"
      ) {
        try {
          cleanMetadata
          deliverSourceDomain()
          deliverSourceTable("/sample/privacy/orders.sl.yml")
          loadPending.isSuccess shouldBe true

          sparkSession
            .sql(
              "SELECT id, customer.email, customer.city, cards, payload, channel FROM privacy.orders"
            )
            .collect()
            .toList
            .map(row =>
              (
                row.getLong(0),
                row.getString(1),
                row.getString(2),
                row.getSeq[String](3),
                row.getString(4),
                row.getString(5)
              )
            ) shouldBe List(
            (
              1L,
              sha256("alice@example.com"),
              "Paris",
              Seq(md5("4111"), md5("5500")),
              // A JSON string
              "\"" + sha256("""{"note":"x"}""") + "\"",
              // The default of a missing column
              sha256("web")
            )
          )
        } finally {
          sparkSession.sql("DROP TABLE IF EXISTS privacy.orders")
        }
      }
    }
  }

  new WithSettings() {
    "The Snowflake orchestrator" should "refuse a table whose privacy its SQL cannot apply" in {
      val schemaHandler = settings.schemaHandler()
      val job = new DummyIngestionJob(
        domain = DomainInfo("privacy"),
        schema = SchemaInfo(
          "customers",
          Pattern.compile("customers.*"),
          List(
            TableAttribute("id", "long"),
            TableAttribute("email", privacy = Some(TransformInput("SHA256", sql = false)))
          )
        ),
        types = schemaHandler.types(),
        path = Nil,
        storageHandler = settings.storageHandler(),
        schemaHandler = schemaHandler,
        options = Map.empty,
        accessToken = None,
        test = false,
        scheduledDate = None
      )
      val error =
        intercept[IllegalArgumentException](job.buildListOfSQLStatementsAsMap("snowflake"))
      error.getMessage should include("because of the privacy of email")
    }
  }

  private val customPrivacyConfiguration: Config =
    ConfigFactory
      .parseString("""privacy.options.hide3star = "ai.starlake.utils.Hide(\"*\",3)"""")
      .withFallback(testConfiguration)

  new WithSettings(customPrivacyConfiguration) {
    "A privacy level declared in privacy.options" should "be applied" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/privacy/_config.sl.yml",
        datasetDomainName = "privacy",
        sourceDatasetPathName = "/sample/privacy/custom-1.dsv"
      ) {
        // The privacy levels are cached by the first load of the JVM
        PrivacyLevels.resetAllPrivacy()
        try {
          cleanMetadata
          deliverSourceDomain()
          deliverSourceTable("/sample/privacy/custom.sl.yml")
          loadPending.isSuccess shouldBe true

          sparkSession
            .sql("SELECT secret FROM privacy.custom")
            .collect()
            .map(_.getString(0))
            .toList shouldBe List("***")
        } finally {
          PrivacyLevels.resetAllPrivacy()
          sparkSession.sql("DROP TABLE IF EXISTS privacy.custom")
        }
      }
    }
  }

  "nonSqlPrivacyAttributes" should "list the loaded leaf attributes whose privacy the native loaders do not apply" in {
    IngestionJob.nonSqlPrivacyAttributes(
      List(
        TableAttribute("id", "long"),
        TableAttribute("none", privacy = Some(TransformInput.None)),
        TableAttribute("sql", privacy = Some(TransformInput("UPPER(sql)", sql = true))),
        TableAttribute("email", privacy = Some(TransformInput("SHA256", sql = false))),
        TableAttribute("payload", "variant", privacy = Some(TransformInput("MD5", sql = false))),
        // Not loaded
        TableAttribute(
          "ignored",
          privacy = Some(TransformInput("MD5", sql = false)),
          ignore = Some(true)
        ),
        // Not applied by any loader
        TableAttribute(
          "scripted",
          privacy = Some(TransformInput("MD5", sql = false)),
          script = Some("'x'")
        ),
        TableAttribute(
          "customer",
          "struct",
          // Only leaves are transformed
          privacy = Some(TransformInput("HIDE", sql = false)),
          attributes = List(
            TableAttribute("name", privacy = Some(TransformInput("HIDE", sql = false))),
            TableAttribute("city")
          )
        )
      )
    ) shouldBe List("email", "payload", "customer.name")
  }
}
