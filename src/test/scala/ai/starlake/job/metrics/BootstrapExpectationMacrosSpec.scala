package ai.starlake.job.metrics

import ai.starlake.TestHelper
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Utils
import better.files.File

import java.sql.DriverManager
import scala.util.{Failure, Success, Try, Using}

/** The expectation macros that `bootstrap` ships follow the rule of `ExpectationJob` (#1879): one
  * row holding one integer, 0 meaning that the expectation passed. Each macro runs on DuckDB, the
  * engine of the bootstrapped projects, against a table where its rule holds and one where it is
  * broken.
  */
class BootstrapExpectationMacrosSpec extends TestHelper {

  private def expectations(template: String): File =
    File(
      getClass
        .getResource(s"/templates/bootstrap/samples/$template/metadata/expectations")
        .toURI
    )

  private val macroDefinition = """\{%-?\s*macro\s+(\w+)""".r

  private def definedMacros(directory: File): List[String] =
    directory.list
      .filter(_.name.endsWith(".j2"))
      .toList
      .flatMap(file => macroDefinition.findAllMatchIn(file.contentAsString).map(_.group(1)))

  /** A rule, called on the table where it holds and on the table where it is broken. `sl_this`
    * stands for `dq.ok` in the first call and `dq.ko` in the second one, as `ExpectationJob`
    * replaces it with the table of the expectation.
    */
  private case class Rule(name: String, holds: String, broken: String)

  private object Rule {
    def apply(name: String, arguments: String): Rule =
      Rule(name, s"$name($arguments)", s"$name($arguments)")
  }

  private val rules = List(
    // Completeness
    Rule("expect_column_values_to_be_null", "'sl_this', 'empty', 0.9"),
    Rule("expect_column_values_to_not_be_null", "'sl_this', 'filled'"),
    // Numeric
    Rule(
      "expect_column_kl_divergence_to_be_less_than",
      """'sl_this', 'grade', {"'A'": 0.6667, "'B'": 0.3333}, 0.1"""
    ),
    Rule("expect_column_max_to_be_between", "'sl_this', 'amount', 0, 100"),
    Rule("expect_column_mean_to_be_between", "'sl_this', 'amount', 0, 100"),
    Rule("expect_column_median_to_be_between", "'sl_this', 'amount', 15, 25"),
    Rule("expect_column_min_to_be_between", "'sl_this', 'amount', 5, 15"),
    Rule("expect_column_pair_values_a_to_be_greater_than_b", "'sl_this', 'x', 'y'"),
    Rule(
      "expect_column_quantile_values_to_be_between",
      "'sl_this', 'amount', [25, 75], [15, 25], [25, 35]"
    ),
    Rule("expect_column_stdev_to_be_between", "'sl_this', 'amount', 5, 15"),
    Rule("expect_column_sum_to_be_between", "'sl_this', 'amount', 0, 100"),
    Rule("expect_column_value_z_scores_to_be_less_than", "'sl_this', 'amount', 1.1"),
    // A value far below the mean counts too, unless the check is one-sided
    Rule("expect_column_value_z_scores_to_be_less_than", "'sl_this', '-amount', 1.1"),
    Rule(
      "expect_column_value_z_scores_to_be_less_than",
      "expect_column_value_z_scores_to_be_less_than('dq.ko', '-amount', 1.1, False)",
      "expect_column_value_z_scores_to_be_less_than('dq.ko', 'amount', 1.1, False)"
    ),
    Rule("expect_column_values_to_be_between", "'sl_this', 'amount', 0, 100"),
    Rule("expect_multicolumn_sum_to_equal", "'sl_this', ['s1', 's2'], 10"),
    // Schema
    Rule(
      "expect_column_to_exist",
      "expect_column_to_exist('dq.ok', 'code')",
      "expect_column_to_exist('dq.ok', 'missing')"
    ),
    Rule(
      "expect_column_values_to_be_in_type_list",
      "expect_column_values_to_be_in_type_list('dq.ok', 'code', ['VARCHAR', 'BLOB'])",
      "expect_column_values_to_be_in_type_list('dq.ok', 'code', ['INTEGER', 'BLOB'])"
    ),
    Rule(
      "expect_column_values_to_be_of_type",
      "expect_column_values_to_be_of_type('dq.ok', 'id', 'INTEGER')",
      "expect_column_values_to_be_of_type('dq.ok', 'id', 'VARCHAR')"
    ),
    Rule(
      "expect_table_column_count_to_be_between",
      "expect_table_column_count_to_be_between('dq.small', 1, 2)",
      "expect_table_column_count_to_be_between('dq.small', 3, 4)"
    ),
    Rule(
      "expect_table_column_count_to_equal",
      "expect_table_column_count_to_equal('dq.small', 2)",
      "expect_table_column_count_to_equal('dq.small', 3)"
    ),
    Rule(
      "expect_table_columns_to_match_ordered_list",
      "expect_table_columns_to_match_ordered_list('dq.small', [['k', 1], ['v', 2]])",
      "expect_table_columns_to_match_ordered_list('dq.small', [['v', 1], ['k', 2]])"
    ),
    Rule(
      "expect_table_columns_to_match_set",
      "expect_table_columns_to_match_set('dq.small', ['v', 'k'])",
      "expect_table_columns_to_match_set('dq.small', ['k', 'w'])"
    ),
    // Uniqueness
    Rule("expect_column_distinct_values_to_be_in_set", """'sl_this', 'code', ["'A01'", "'A02'"]"""),
    Rule("expect_column_distinct_values_to_contain_set", "'sl_this', 'code', ['A01', 'A02']"),
    Rule("expect_column_distinct_values_to_equal_set", "'sl_this', 'code', ['A01', 'A02']"),
    Rule("expect_column_proportion_of_unique_values_to_be_between", "'sl_this', 'id', 1, 1"),
    Rule("expect_column_unique_value_count_to_be_between", "'sl_this', 'id', 3, 3"),
    Rule("expect_column_values_to_be_unique", "'sl_this', 'id'"),
    Rule("expect_compound_columns_to_be_unique", "'sl_this', ['id', 'code']"),
    Rule("expect_select_column_values_to_be_unique_within_record", "'sl_this', ['x', 'y']"),
    // Validity
    Rule("expect_column_most_common_value_to_be_in_set", "'sl_this', 'grade', ['A']"),
    // Strings are quoted, their quotes doubled
    Rule("expect_column_most_common_value_to_be_in_set", """'sl_this', 'grade', ['A', "O'K"]"""),
    Rule(
      "expect_column_most_common_value_to_be_in_set",
      "expect_column_most_common_value_to_be_in_set('dq.tie', 'grade', ['A'], True)",
      "expect_column_most_common_value_to_be_in_set('dq.tie', 'grade', ['A'])"
    ),
    Rule("expect_column_pair_values_to_be_equal", "'sl_this', 'a', 'b'"),
    Rule("expect_column_pair_values_to_be_in_set", "'sl_this', 'a', 'b', [[1, 1], [2, 2], [3, 3]]"),
    // Strings are quoted, their quotes doubled
    Rule(
      "expect_column_pair_values_to_be_in_set",
      """'sl_this', 'code', 'grade', [['A01', 'A'], ['A02', 'A'], ['A01', 'B'], ["O'K", 'B']]"""
    ),
    // None is NULL, compared as a value
    Rule("expect_column_pair_values_to_be_in_set", "'sl_this', 'empty', 'filled', [[None, 'f']]"),
    Rule("expect_column_value_lengths_to_be_between", "'sl_this', 'label', 4, 4"),
    Rule("expect_column_value_lengths_to_equal", "'sl_this', 'label', 4"),
    Rule("expect_column_values_to_be_in_set", """'sl_this', 'code', ["'A01'", "'A02'"]"""),
    Rule("expect_column_values_to_match_like_pattern", "'sl_this', 'label', 'ab-%'"),
    Rule(
      "expect_column_values_to_match_like_pattern_list",
      "'sl_this', 'label', ['ab-%', 'xy-%']"
    ),
    Rule("expect_column_values_to_match_regex", "'sl_this', 'label', '^ab-[0-9]$'"),
    // `mostly` as a percentage, as before
    Rule("expect_column_values_to_match_regex", "'sl_this', 'label', '^ab-[0-9]$', 95"),
    Rule("expect_column_values_to_match_regex_list", "'sl_this', 'label', ['^ab-', '^xy-']"),
    Rule("expect_column_values_to_not_be_in_set", """'sl_this', 'code', ["'Z99'"]"""),
    Rule("expect_column_values_to_not_match_like_pattern", "'sl_this', 'label', 'zz%'"),
    Rule(
      "expect_column_values_to_not_match_like_pattern_list",
      "'sl_this', 'label', ['zz%', 'yy%'], 0.9"
    ),
    Rule("expect_column_values_to_not_match_regex", "'sl_this', 'label', '^zz'"),
    Rule("expect_column_values_to_not_match_regex_list", "'sl_this', 'label', ['^zz', '^yy']"),
    // No pattern
    Rule(
      "expect_column_values_to_not_match_regex_list",
      "expect_column_values_to_not_match_regex_list('dq.ok', 'label', [])",
      "expect_column_values_to_match_regex_list('dq.ok', 'label', [])"
    ),
    // Volume
    Rule(
      "expect_table_row_count_to_be_between",
      "expect_table_row_count_to_be_between('dq.ok', 3, 3)",
      "expect_table_row_count_to_be_between('dq.ok', 4, 5)"
    ),
    Rule(
      "expect_table_row_count_to_equal",
      "expect_table_row_count_to_equal('dq.ok', 3)",
      "expect_table_row_count_to_equal('dq.ok', 4)"
    ),
    Rule(
      "expect_table_row_count_to_equal_other_table",
      "expect_table_row_count_to_equal_other_table('dq.ok', 'dq.ko')",
      "expect_table_row_count_to_equal_other_table('dq.ok', 'dq.small')"
    ),
    // A table compared with itself
    Rule(
      "unexpected_rows_expectation",
      "unexpected_rows_expectation('sl_this', 'dq.ok', 'code')",
      "unexpected_rows_expectation('sl_this', 'dq.ok', 'code')"
    )
  )

  private val tables = List(
    "CREATE SCHEMA dq",
    """CREATE TABLE dq.ok (id INT, code VARCHAR, grade VARCHAR, amount DOUBLE, a INT, b INT,
      |x INT, y INT, s1 INT, s2 INT, label VARCHAR, empty VARCHAR, filled VARCHAR)""".stripMargin,
    """INSERT INTO dq.ok VALUES
      |(1, 'A01', 'A', 10, 1, 1, 5, 1, 3, 7, 'ab-1', NULL, 'f'),
      |(2, 'A02', 'A', 20, 2, 2, 6, 2, 4, 6, 'ab-2', NULL, 'f'),
      |(3, 'A01', 'B', 30, 3, 3, 7, 3, 5, 5, 'ab-3', NULL, 'f')""".stripMargin,
    "CREATE TABLE dq.ko AS SELECT * FROM dq.ok WHERE FALSE",
    """INSERT INTO dq.ko VALUES
      |(1, 'A01', 'B', 1, 1, 1, 5, 1, 3, 7, 'ab-1', 'e', 'f'),
      |(1, 'A01', 'B', 1, 2, 9, 2, 2, 4, 7, 'zz', 'e', NULL),
      |(3, 'Z99', 'B', 1000, 3, 3, 7, 3, 5, 5, 'ab-3', NULL, 'f')""".stripMargin,
    "CREATE TABLE dq.small (k INT, v INT)",
    "INSERT INTO dq.small VALUES (1, 1), (2, 2)",
    // Two most common values, one of them outside the set
    "CREATE TABLE dq.tie (grade VARCHAR)",
    "INSERT INTO dq.tie VALUES ('A'), ('Z')"
  )

  new WithSettings() {
    private val macros = {
      val directory = expectations("empty-project")
      SQLUtils.stripComments(
        directory.list
          .filter(_.name.endsWith(".j2"))
          .toList
          .sortBy(_.name)
          .map(file => "\n" + file.contentAsString + "\n")
          .mkString("\n")
      )
    }

    /** Runs a macro call as `ExpectationJob` does: one row, one integer */
    private def run(call: String, table: String)(implicit connection: java.sql.Connection): Int = {
      val sql = Utils
        .parseJinja(macros + "\n{{ " + call + " }}", Map.empty[String, Any])
        .replaceAll("(?i)sl_this", table)
      new SQLConnectionExpectationAssertionHandler(connection).handle(sql)
    }

    "The expectation macros of bootstrap" should "return 0 when their rule holds, a positive number when it is broken" in {
      Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { implicit connection =>
        Using.resource(connection.createStatement())(statement => tables.foreach(statement.execute))
        // Every failing call, reported together
        val failures = rules.flatMap { rule =>
          List(
            (rule.holds, "dq.ok", (result: Int) => result == 0),
            (rule.broken, "dq.ko", (result: Int) => result > 0)
          ).flatMap { case (call, table, expected) =>
            Try(run(call, table)) match {
              case Success(result) if expected(result) => None
              case Success(result)                     => Some(s"$call on $table: $result")
              case Failure(exception) => Some(s"$call on $table: ${exception.getMessage}")
            }
          }
        }
        failures shouldBe Nil
      }
    }

    it should "all be tested" in {
      definedMacros(expectations("empty-project")).toSet shouldBe rules.map(_.name).toSet
    }

    it should "define each macro once" in {
      // The definition read last would replace the others
      val names = definedMacros(expectations("empty-project"))
      names.diff(names.distinct) shouldBe Nil
    }

    it should "be the same in the sample project" in {
      val sample = expectations("sample-project")
      val empty = expectations("empty-project")
      def sampleName(name: String): String =
        if (name == "UnexpectedRowsExpectation.j2") "UnexpectedRowsExpectation.sql.j2" else name
      sample.list.map(_.name).toSet shouldBe empty.list.map(file => sampleName(file.name)).toSet
      empty.list.map(_.name).toList.sorted.foreach { name =>
        withClue(name) {
          // The sample project names one macro in CamelCase
          (sample / sampleName(name)).contentAsString.replace(
            "ExpectColumnDistinctValuesToBeInSet",
            "expect_column_distinct_values_to_be_in_set"
          ) shouldBe (empty / name).contentAsString
        }
      }
    }
  }
}
