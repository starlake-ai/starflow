package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AllSinks, SchemaInfo, TableAttribute}
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern

/** What makes a native load take two steps, and what its temporary tables and its archive table
  * inherit from its target (#1838)
  */
class BigQueryNativeTwoStepsSpec extends TestHelper {

  private val localFiles = List(new Path("file:/tmp/a.csv"), new Path("file:/tmp/b.csv"))
  private val gcsFiles = List(new Path("gs://bucket/a.csv"), new Path("gs://bucket/b.csv"))

  private val schema =
    SchemaInfo(
      "orders",
      Pattern.compile("orders.*"),
      attributes = List(
        TableAttribute("id", "long", rename = Some("order_id")),
        TableAttribute("customer", "string", foreignKey = Some("customers.id"))
      ),
      primaryKey = List("order_id")
    )

  "requiresOneLoadPerFile" should "load several local files one by one, whatever their format" in {
    BigQueryNativeLoader.requiresOneLoadPerFile(localFiles) shouldBe true
    BigQueryNativeLoader.requiresOneLoadPerFile(
      List(new Path("file:/tmp/a.parquet"), new Path("file:/tmp/b.parquet"))
    ) shouldBe true
    BigQueryNativeLoader.requiresOneLoadPerFile(localFiles.take(1)) shouldBe false
    // A single load job reads several Cloud Storage URIs
    BigQueryNativeLoader.requiresOneLoadPerFile(gcsFiles) shouldBe false
  }

  "secondStepRequired" should "be true when the table has presql or postsql, as the second step runs them" in {
    BigQueryNativeLoader.secondStepRequired(schema, gcsFiles) shouldBe false
    BigQueryNativeLoader.secondStepRequired(
      schema.copy(presql = List("DELETE FROM t WHERE TRUE")),
      gcsFiles
    ) shouldBe true
    BigQueryNativeLoader.secondStepRequired(
      schema.copy(postsql = List("SELECT 1")),
      gcsFiles
    ) shouldBe true
  }

  it should "ignore empty statements" in {
    BigQueryNativeLoader.secondStepRequired(
      schema.copy(presql = List(""), postsql = List("  ")),
      gcsFiles
    ) shouldBe false
  }

  it should "be true for several local files" in {
    BigQueryNativeLoader.secondStepRequired(schema, localFiles) shouldBe true
    BigQueryNativeLoader.secondStepRequired(schema, localFiles.take(1)) shouldBe false
  }

  "withoutKeys" should "remove the primary key and the foreign keys, which name the target columns" in {
    val result = BigQueryNativeLoader.withoutKeys(schema)
    result.primaryKey shouldBe Nil
    result.attributes.flatMap(_.foreignKey) shouldBe Nil
    // The rest is kept
    result.attributes.map(_.name) shouldBe List("id", "customer")
    result.attributes.flatMap(_.rename) shouldBe List("order_id")
  }

  "archiveSink" should "keep the partition and the clustering of the target on the columns of the archive table only" in {
    val sink = AllSinks(partition = Some(Seq("day")), clustering = Some(Seq("country", "city")))
    NativeLoader.archiveSink(sink, List("ID", "DAY", "COUNTRY", "CITY")) shouldBe sink
    // A renamed partition column, or a POSITION file, whose temporary table has a `value` column
    NativeLoader.archiveSink(sink, List("value")) shouldBe
    sink.copy(partition = None, clustering = None)
    NativeLoader.archiveSink(sink, List("day", "country")) shouldBe sink.copy(clustering = None)
  }

  it should "keep a partition by ingestion time or on an expression" in {
    val byIngestionTime = AllSinks(partition = Some(Seq("_PARTITIONTIME")))
    NativeLoader.archiveSink(byIngestionTime, List("value")) shouldBe byIngestionTime
    val onExpression = AllSinks(partition = Some(Seq("DATE(ts)")))
    NativeLoader.archiveSink(onExpression, List("value")) shouldBe onExpression
  }

  it should "read a column name between backquotes, which may hold any character" in {
    val sink = AllSinks(partition = Some(Seq("`sale-day`")))
    NativeLoader.archiveSink(sink, List("SALE-DAY")) shouldBe sink
    NativeLoader.archiveSink(sink, List("value")) shouldBe sink.copy(partition = None)
  }
}
