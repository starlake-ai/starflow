package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.job.ingest.loaders.BigQueryNativeLoader
import ai.starlake.schema.model.{AccessControlEntry, RowLevelSecurity}
import com.google.cloud.bigquery.{Field, JobInfo, PolicyTags, StandardSQLTypeName, TableId}

import scala.jdk.CollectionConverters._

class BigQueryAccessPoliciesSpec extends TestHelper {

  private val target = BigQueryLoadConfig(
    connectionRef = Some("bigquery"),
    source = Left("gs://bucket/a.csv,gs://bucket/b.csv"),
    outputTableId = Some(TableId.of("project", "domain", "table")),
    outputPartition = Some("day"),
    days = Some(30),
    requirePartitionFilter = true,
    writeDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE.name(),
    rls = List(RowLevelSecurity("only_fr", "country = 'FR'", Set("group:fr@example.com"))),
    acl = List(AccessControlEntry("roles/bigquery.dataViewer", Set("group:all@example.com"))),
    accessToken = Some("token")
  )

  "firstStepConfig" should "load a file into its temporary table without access policies or partition filter" in {
    val tempTable = TableId.of("project", "domain", "zztmp_table_0")
    val config = BigQueryNativeLoader.firstStepConfig(target, "gs://bucket/b.csv", tempTable)
    config.source shouldBe Left("gs://bucket/b.csv")
    config.outputTableId shouldBe Some(tempTable)
    config.writeDisposition shouldBe JobInfo.WriteDisposition.WRITE_APPEND.name()
    // The second step reads every row and column of it
    config.rls shouldBe Nil
    config.acl shouldBe Nil
    config.requirePartitionFilter shouldBe false
    // As the target otherwise
    config.outputPartition shouldBe Some("day")
    config.days shouldBe Some(30)
    config.accessToken shouldBe Some("token")
  }

  private def tags(field: Field): List[String] =
    Option(field.getPolicyTags).map(_.getNames.asScala.toList).getOrElse(Nil)

  private def tagged(field: Field, tag: String): Field =
    field.toBuilder
      .setPolicyTags(PolicyTags.newBuilder().setNames(List(tag).asJava).build())
      .build()

  "withPolicyTags" should "tag a column whatever the case of its name, and keep its other properties" in {
    val customerId = Field
      .newBuilder("CustomerId", StandardSQLTypeName.STRING)
      .setMode(Field.Mode.REQUIRED)
      .setDescription("The id of the customer")
      .setMaxLength(12L)
      .build()
    val List(result) = BigQueryJobBase.withPolicyTags(
      "table",
      List(customerId),
      List("customerID" -> Some("PII")),
      policy => s"tags/${policy.toLowerCase}"
    )
    tags(result) shouldBe List("tags/pii")
    result.getName shouldBe "CustomerId"
    result.getMode shouldBe Field.Mode.REQUIRED
    result.getDescription shouldBe "The id of the customer"
    result.getMaxLength shouldBe 12L
  }

  it should "replace another policy tag" in {
    val email = tagged(Field.of("email", StandardSQLTypeName.STRING), "tags/old")
    val List(result) =
      BigQueryJobBase.withPolicyTags(
        "table",
        List(email),
        List("email" -> Some("PII")),
        _ => "tags/pii"
      )
    tags(result) shouldBe List("tags/pii")
  }

  it should "keep the same columns when they have their tag or no access policy" in {
    val email = tagged(Field.of("email", StandardSQLTypeName.STRING), "tags/pii")
    val amount = Field.of("amount", StandardSQLTypeName.NUMERIC)
    val ignored = Field.of("ignored", StandardSQLTypeName.STRING)
    val fields = List(email, amount, ignored)
    val result = BigQueryJobBase.withPolicyTags(
      "table",
      fields,
      List("email" -> Some("PII"), "amount" -> None),
      _ => "tags/pii"
    )
    result.zip(fields).foreach { case (kept, field) => kept should be theSameInstanceAs field }
  }
}
