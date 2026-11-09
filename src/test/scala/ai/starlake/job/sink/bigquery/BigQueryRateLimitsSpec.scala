package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.schema.model.RowLevelSecurity
import com.google.cloud.bigquery.{BigQuery, BigQueryError, BigQueryException, FakeBigQuery, TableId}

import scala.jdk.CollectionConverters._
import scala.util.Try

class BigQueryRateLimitsSpec extends TestHelper {

  private val rateLimit = new BigQueryException(
    403,
    "Job exceeded rate limits: Your table exceeded quota for table update operations",
    new BigQueryError("jobRateLimitExceeded", "", "Job exceeded rate limits")
  )

  private val jobRateLimit = Some("jobRateLimitExceeded")

  "waitForQueryJob" should "surface the failure of a job on a rate limit, without polling it again" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => throw rateLimit
      case "getJob"          => fake.queryJob("DONE", jobRateLimit)
    })
    val failure = Try(BigQueryNativeJob.waitForQueryJob(fake.queryJob("DONE"), 60000L)).failed.get
    fake.calls.count(_ == "getQueryResults") shouldBe 1
    // recoverBigqueryException then runs the statement as a new job
    BigQueryJobBase.isRetryableException(failure) shouldBe true
  }

  it should "surface the failure of a job that the polling reports as complete" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => fake.queryResponse(completed = true)
      case "getJob"          => fake.queryJob("DONE", jobRateLimit)
    })
    val failure =
      Try(BigQueryNativeJob.waitForQueryJob(fake.queryJob("RUNNING"), 60000L)).failed.get
    BigQueryJobBase.isRetryableException(failure) shouldBe true
  }

  it should "not run again a script that failed on a rate limit, which may have committed statements" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => throw rateLimit
      case "getJob"          => fake.queryJob("DONE", jobRateLimit, childJobs = Some(2L))
    })
    val failure = Try(BigQueryNativeJob.waitForQueryJob(fake.queryJob("DONE"), 60000L)).failed.get
    failure.getMessage should include("jobRateLimitExceeded")
    BigQueryJobBase.isRetryableException(failure) shouldBe false
  }

  it should "keep waiting for a running job when its polling request is rate-limited" in {
    var polls = 0
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" =>
        polls += 1
        if (polls == 1) throw rateLimit else fake.queryResponse(completed = true)
      case "getJob" => fake.queryJob(if (polls == 1) "RUNNING" else "DONE")
    })
    BigQueryNativeJob
      .waitForQueryJob(fake.queryJob("RUNNING"), 60000L)
      .getJobId
      .getJob shouldBe "job"
    polls shouldBe 2
  }

  it should "keep waiting when the state of the job cannot be read either" in {
    var polls = 0
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" =>
        polls += 1
        if (polls == 1) throw rateLimit else fake.queryResponse(completed = true)
      case "getJob" =>
        if (polls == 1) throw rateLimit else fake.queryJob("DONE")
    })
    BigQueryNativeJob
      .waitForQueryJob(fake.queryJob("RUNNING"), 60000L)
      .getJobId
      .getJob shouldBe "job"
  }

  it should "return a job that succeeded, when the polling request is rate-limited" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => throw rateLimit
      case "getJob"          => fake.queryJob("DONE")
    })
    BigQueryNativeJob
      .waitForQueryJob(fake.queryJob("RUNNING"), 60000L)
      .getJobId
      .getJob shouldBe "job"
  }

  it should "propagate an error that is not transient" in {
    val invalid = new BigQueryException(400, "Syntax error")
    lazy val fake: FakeBigQuery = new FakeBigQuery({ case "getQueryResults" => throw invalid })
    Try(
      BigQueryNativeJob.waitForQueryJob(fake.queryJob("DONE"), 60000L)
    ).failed.get shouldBe invalid
    fake.calls should not contain "getJob"
  }

  it should "read its timeout as milliseconds, and cancel a statement still running without retrying it" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => fake.queryResponse(completed = false)
      case "getJob"          => fake.queryJob("RUNNING")
      case "cancel"          => java.lang.Boolean.TRUE
    })
    val start = System.currentTimeMillis()
    val failure = Try(BigQueryNativeJob.waitForQueryJob(fake.queryJob("RUNNING"), 200L)).failed.get
    System.currentTimeMillis() - start should be < 30000L
    failure.getMessage should include("still running after 200 ms")
    failure.getMessage should include("cancellation was requested")
    fake.calls.count(_ == "cancel") shouldBe 1
    BigQueryJobBase.isRetryableException(failure) shouldBe false
  }

  it should "not cancel a script still running, which could stop halfway" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => fake.queryResponse(completed = false)
      case "getJob"          => fake.queryJob("RUNNING", childJobs = Some(1L))
    })
    val failure = Try(BigQueryNativeJob.waitForQueryJob(fake.queryJob("RUNNING"), 200L)).failed.get
    failure.getMessage should include("was not cancelled")
    fake.calls should not contain "cancel"
    BigQueryJobBase.isRetryableException(failure) shouldBe false
  }

  it should "not claim a cancellation that BigQuery refused" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => fake.queryResponse(completed = false)
      case "getJob"          => fake.queryJob("RUNNING")
      case "cancel"          => throw rateLimit
    })
    Try(
      BigQueryNativeJob.waitForQueryJob(fake.queryJob("RUNNING"), 200L)
    ).failed.get.getMessage should include("was not cancelled")
  }

  it should "fail on a job that no longer exists" in {
    lazy val fake: FakeBigQuery = new FakeBigQuery({
      case "getQueryResults" => fake.queryResponse(completed = true)
      case "getJob"          => null
    })
    Try(
      BigQueryNativeJob.waitForQueryJob(fake.queryJob("RUNNING"), 60000L)
    ).failed.get.getMessage should include("no longer exists")
  }

  "labelsChanged" should "ignore the labels that are not declared, which an update would not remove" in {
    BigQueryJobBase.labelsChanged(Map("owner" -> "data").asJava, Map.empty) shouldBe false
    BigQueryJobBase.labelsChanged(
      Map("owner" -> "data", "team" -> "a").asJava,
      Map("team" -> "a")
    ) shouldBe false
    BigQueryJobBase.labelsChanged(null, Map.empty) shouldBe false
  }

  it should "detect a declared label that is missing or has another value" in {
    BigQueryJobBase.labelsChanged(Map("owner" -> "data").asJava, Map("team" -> "a")) shouldBe true
    BigQueryJobBase.labelsChanged(Map("team" -> "b").asJava, Map("team" -> "a")) shouldBe true
    BigQueryJobBase.labelsChanged(null, Map("team" -> "")) shouldBe true
  }

  private val tableId = TableId.of("project", "domain", "table")

  private def job(fake: FakeBigQuery, config: BigQueryLoadConfig)(implicit
    testSettings: Settings
  ): BigQueryJobBase = new BigQueryJobBase {
    override def settings: Settings = testSettings
    override def cliConfig: BigQueryLoadConfig = config
    override def bigquery(alwaysCreate: Boolean, accessToken: Option[String])(implicit
      settings: Settings
    ): BigQuery = fake.service
  }

  private val config =
    BigQueryLoadConfig(connectionRef = Some("bigquery"), outputTableId = Some(tableId))

  new WithSettings() {
    "getOrCreateDataset" should "not update a dataset that only has other labels" in {
      lazy val fake: FakeBigQuery = new FakeBigQuery({ case "getDataset" =>
        fake.dataset(Some("comment"), Map("owner" -> "terraform"))
      })
      job(fake, config).getOrCreateDataset(Some("comment")).isSuccess shouldBe true
      fake.calls should not contain "update"
    }

    it should "update a dataset whose description or declared labels differ" in {
      lazy val fake: FakeBigQuery = new FakeBigQuery({
        case "getDataset" => fake.dataset(Some("comment"), Map.empty)
        case "update"     => fake.dataset(Some("comment"), Map("team" -> "a"))
      })
      job(fake, config.copy(domainTags = Set("team=a")))
        .getOrCreateDataset(Some("comment"))
        .isSuccess shouldBe true
      fake.calls.count(_ == "update") shouldBe 1
    }

    it should "not fail when another load updates the dataset at the same time" in {
      lazy val fake: FakeBigQuery = new FakeBigQuery({
        case "getDataset" => fake.dataset(Some("old"), Map.empty)
        case "update" =>
          throw new BigQueryException(412, "Precondition check failed: IF_MATCH")
      })
      job(fake, config).getOrCreateDataset(Some("comment")).isSuccess shouldBe true
      fake.calls.count(_ == "update") shouldBe 1
    }

    it should "not update a dataset for an empty comment, that BigQuery does not store" in {
      lazy val fake: FakeBigQuery = new FakeBigQuery({ case "getDataset" =>
        fake.dataset(None, Map.empty)
      })
      job(fake, config).getOrCreateDataset(Some("")).isSuccess shouldBe true
      fake.calls should not contain "update"
    }

    {
      val unmanaged: Settings =
        settings.copy(appConfig = settings.appConfig.copy(createSchemaIfNotExists = false))

      it should "neither update nor create a dataset when createSchemaIfNotExists is false" in {
        lazy val existing: FakeBigQuery = new FakeBigQuery({ case "getDataset" =>
          existing.dataset(Some("old"), Map.empty)
        })
        job(existing, config)(unmanaged)
          .getOrCreateDataset(Some("comment"))(unmanaged)
          .isSuccess shouldBe true
        existing.calls should not contain "update"

        val missing = new FakeBigQuery({ case "getDataset" => null })
        job(missing, config)(unmanaged)
          .getOrCreateDataset(Some("comment"))(unmanaged)
          .failed
          .get
          .getMessage should include("createSchemaIfNotExists")
        missing.calls should not contain "create"
      }
    }
  }

  "buildRLSQueries" should "replace each policy, and drop none" in {
    val statements = BigQueryJobBase.buildRLSQueries(
      tableId,
      List(
        RowLevelSecurity("first", "a = 1", Set("user:a@example.com")),
        RowLevelSecurity("second", "b = 1", Set("group:b@example.com"))
      )
    )
    statements.map(_.trim.split("\\s+").take(5).mkString(" ")) shouldBe List.fill(2)(
      "CREATE OR REPLACE ROW ACCESS"
    )
    statements.exists(_.contains("DROP")) shouldBe false
  }

  "buildRLSDropQueries" should "drop only the policies that are no longer declared" in {
    BigQueryJobBase.buildRLSDropQueries(
      tableId,
      List(RowLevelSecurity("Kept", "a = 1", Set("user:a@example.com"))),
      List("kept", "stale")
    ) shouldBe List("DROP ROW ACCESS POLICY IF EXISTS `stale` ON `project.domain.table`")
  }
}
