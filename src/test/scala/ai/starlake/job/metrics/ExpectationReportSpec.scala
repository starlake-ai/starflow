package ai.starlake.job.metrics

import ai.starlake.TestHelper
import ai.starlake.schema.model.Engine

import java.sql.Timestamp
import java.time.Instant

class ExpectationReportSpec extends TestHelper {

  new WithSettings() {

    "ExpectationReport.asSelect" should
    "escape quotes, jinja delimiters and backslashes without failing the Jinja pass" in {
      val report = ExpectationReport(
        jobId = "job'1",
        database = Some("db'name"),
        domain = "dom'ain",
        schema = "sch'ema",
        timestamp = Timestamp.from(Instant.now()),
        name = "it's a name with a trailing \\",
        params = "params {% ANUM %} end",
        sql = Some("SELECT 1 {# comment #}"),
        count = Some(1L),
        // {}}%ANUM%#}} carries no delimiter of its own: pairwise stripping of {{ }} / {% %} /
        // {# #} is not a fixpoint and used to synthesize {%ANUM%} out of it, which is exactly
        // the kind of value this field records (a rendered exception message can contain
        // anything the failing SQL engine echoed back).
        exception = Some("boom {}}%ANUM%#}} end"),
        success = false
      )

      val select = report.asSelect(Engine.JDBC)

      // name and exception went through the local replaceQuote before this change; params and
      // sql exercise the {% %} and {# #} delimiters that replaceQuote never stripped at all.
      select should include("it\"s a name with a trailing -")
      select should include("params % ANUM % end")
      select should include("SELECT 1 # comment #")
      select should include("boom %ANUM%# end")

      // jobid, database, domain and schema used to be inlined raw; they now go through the
      // default (one argument) escapeLiteral, so a quote in any of them becomes a dash, not a
      // double quote.
      select should include("job-1")
      select should include("db-name")
      select should include("dom-ain")
      select should include("sch-ema")

      select should not include "{"
      select should not include "}"
      select should not include "\\"
    }
  }
}
