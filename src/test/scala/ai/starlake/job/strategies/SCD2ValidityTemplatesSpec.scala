package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AllSinks, Materialization, WriteStrategy, WriteStrategyType}

import java.util.regex.Pattern

/** The SCD2 templates add only the validity columns that the SELECT does not return (#1884), with
  * the type of each column, and write dates into date columns (#1885)
  */
class SCD2ValidityTemplatesSpec extends TestHelper {
  new WithSettings() {
    private val select = "SELECT id, label, updated_at FROM dom.source"

    /** The strategy builders, each with an engine */
    private val builders = List(
      "bigquery"   -> "bigquery",
      "duckdb"     -> "duckdb",
      "jdbc"       -> "h2",
      "mysql"      -> "mysql",
      "postgresql" -> "postgresql",
      "redshift"   -> "redshift",
      "snowflake"  -> "snowflake",
      "spark"      -> "spark",
      "sqlserver"  -> "h2",
      "trino"      -> "h2"
    )

    private def render(
      strategyBuilder: String,
      engine: String,
      tableExists: Boolean,
      columns: List[String] = List("id", "label", "updated_at"),
      startTsType: Option[String] = None,
      endTsType: Option[String] = None,
      startTs: String = "valid_from"
    ): String =
      new TransformStrategiesBuilder().buildTransform(
        WriteStrategy(
          `type` = Some(WriteStrategyType.SCD2),
          key = List("id"),
          timestamp = Some("updated_at"),
          startTs = Some(startTs),
          endTs = Some("valid_to"),
          startTsType = startTsType,
          endTsType = endTsType
        ),
        select,
        TransformStrategiesBuilder.TableComponents("", "dom", "t", columns),
        targetTableExists = tableExists,
        truncate = false,
        materializedView = Materialization.TABLE,
        settings.appConfig.jdbcEngines(engine).copy(strategyBuilder = strategyBuilder),
        AllSinks().getSink()
      )

    private def occurrences(sql: String, text: String): Int =
      sql.split(Pattern.quote(text), -1).length - 1

    /** The validity columns that the CREATE template adds, with their types */
    private def added(sql: String): List[String] =
      "(?i)ALTER TABLE dom\\.t ADD (?:COLUMN )?(?:IF NOT EXISTS )?(\\w+ \\w+)".r
        .findAllMatchIn(sql)
        .map(_.group(1))
        .toList

    private def timestamp(strategyBuilder: String): String =
      if (strategyBuilder == "sqlserver") "DATETIME2" else "TIMESTAMP"

    "The CREATE template of an SCD2 task" should "add both validity columns when the SELECT returns neither" in {
      builders.foreach { case (strategyBuilder, engine) =>
        withClue(strategyBuilder) {
          added(render(strategyBuilder, engine, tableExists = false)) shouldBe List(
            s"valid_from ${timestamp(strategyBuilder)}",
            s"valid_to ${timestamp(strategyBuilder)}"
          )
        }
      }
    }

    it should "not add a validity column that the SELECT returns, whatever its case or quotes" in {
      builders.foreach { case (strategyBuilder, engine) =>
        withClue(strategyBuilder) {
          added(
            render(
              strategyBuilder,
              engine,
              tableExists = false,
              columns = List("id", "label", "updated_at", "\"VALID_FROM\"")
            )
          ) shouldBe List(s"valid_to ${timestamp(strategyBuilder)}")
          added(
            render(
              strategyBuilder,
              engine,
              tableExists = false,
              columns = List("id", "label", "updated_at", "valid_from", "valid_to")
            )
          ) shouldBe Nil
        }
      }
    }

    it should "add the validity columns with the type of the strategy" in {
      builders.foreach { case (strategyBuilder, engine) =>
        withClue(strategyBuilder) {
          added(
            render(strategyBuilder, engine, tableExists = false, startTsType = Some("date"))
          ) shouldBe List("valid_from DATE", s"valid_to ${timestamp(strategyBuilder)}")
        }
      }
    }

    it should "add the columns only if they do not exist on BigQuery" in {
      render("bigquery", "bigquery", tableExists = false) should include(
        "ALTER TABLE dom.t ADD COLUMN IF NOT EXISTS valid_from TIMESTAMP;"
      )
    }

    "The SCD2 template" should "write the time of the run into timestamp columns, as before" in {
      builders.foreach { case (strategyBuilder, engine) =>
        withClue(strategyBuilder) {
          val sql = render(strategyBuilder, engine, tableExists = true)
          sql.toUpperCase should include("CURRENT_TIMESTAMP")
          sql should not include "CAST("
        }
      }
    }

    it should "write the date of the run into date columns" in {
      builders.foreach { case (strategyBuilder, engine) =>
        withClue(strategyBuilder) {
          val sql = render(
            strategyBuilder,
            engine,
            tableExists = true,
            startTsType = Some("date"),
            endTsType = Some("date")
          )
          // Each version that it opens or closes
          occurrences(sql, "CAST(") should be > 0
          occurrences(sql, "CAST(") shouldBe occurrences(sql.toUpperCase, "CURRENT_TIMESTAMP")
          occurrences(sql, " AS DATE)") shouldBe occurrences(sql, "CAST(")
        }
      }
    }

    /** The column lists of the INSERT statements of the SCD2 template, unquoted and lower case */
    private def insertedColumns(sql: String): List[List[String]] =
      "(?is)INSERT\\s*(?:INTO\\s+\\S+\\s*)?\\(([^)]*)\\)".r
        .findAllMatchIn(sql)
        .map(_.group(1).toLowerCase.replaceAll("[\"`\\[\\]\\s]", "").split(",").toList)
        .toList

    it should "insert a validity column that the SELECT returns once, whatever its case or quotes" in {
      builders.foreach { case (strategyBuilder, engine) =>
        List("valid_from", "\"VALID_FROM\"").foreach { selected =>
          withClue(s"$strategyBuilder $selected") {
            val inserts = insertedColumns(
              render(
                strategyBuilder,
                engine,
                tableExists = true,
                columns = List("id", "label", "updated_at", selected)
              )
            )
            inserts should not be empty
            inserts.foreach(_.count(_ == "valid_from") shouldBe 1)
          }
        }
      }
    }

    it should "keep a validity column that is also the timestamp of the strategy among the columns of the SELECT" in {
      builders.foreach { case (strategyBuilder, engine) =>
        withClue(strategyBuilder) {
          val inserts =
            insertedColumns(
              render(strategyBuilder, engine, tableExists = true, startTs = "updated_at")
            )
          inserts should not be empty
          // From the SELECT, and as the start column: the run fails, as before, rather than losing
          // the timestamp of the changes
          inserts.foreach(_.count(_ == "updated_at") shouldBe 2)
        }
      }
    }
  }
}
