package ai.starlake.job.convert

import better.files.File
import ai.starlake.TestHelper
import ai.starlake.schema.model.WriteMode
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode

import scala.io.Source

case class Schema(id: String, customer: String, amount: Double, seller_id: String)

class Parquet2CSVSpec extends TestHelper {
  new WithSettings() {

    val rootDir: File = File.newTemporaryDirectory()
    val outputDir: File = File(rootDir, "output")
    val domainName = "domain"
    val domainPath: File = File(rootDir, domainName).createDirectory()
    val schemaName = "schema"
    val schemaPath: File = File(domainPath, schemaName)

    val config: Parquet2CSVConfig = Parquet2CSVConfig(
      new Path(rootDir.pathAsString),
      Some(new Path(outputDir.pathAsString)),
      Some(domainName),
      Some(schemaName),
      Some(WriteMode.OVERWRITE),
//      deleteSource = false,
      options = Map("sep" -> "|", "header" -> "true")
//      partitions = 1
    )

    def createWithWriteDefaultFormat(): Unit = {
      val data = List(
        Schema("12345", "A009701", 123.65, "AQZERD"),
        Schema("56432", "A000000", 23.8, "AQZERD"),
        Schema("12345", "A009701", 123.6, "AQZERD")
      )
      val dataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data))
      dataFrame.write
        .mode(SaveMode.Overwrite)
        .format(settings.appConfig.defaultWriteFormat)
        .save(schemaPath.pathAsString)
    }

    "Convert schema" should "succeed" in {
      createWithWriteDefaultFormat()
      new Parquet2CSV(config, storageHandler).run()
      val csvFile = File(outputDir.pathAsString, domainName, schemaName + ".csv")
      val result = Utils.withResources(Source.fromFile(csvFile.uri)) { csvFileSource =>
        csvFileSource.getLines().toList
      }
      val expected = List(
        "id|customer|amount|seller_id",
        "12345|A009701|123.6|AQZERD",
        "56432|A000000|23.8|AQZERD",
        "12345|A009701|123.65|AQZERD"
      )
      csvFile.delete()
      rootDir.delete()
      expected should contain theSameElementsAs result
    }
    "Convert schema" should "succeed delete source after completion" in {
      createWithWriteDefaultFormat()
      val inputPath = File(rootDir.pathAsString, domainName, schemaName)
      inputPath.exists shouldBe true
      new Parquet2CSV(config.copy(deleteSource = true), storageHandler).run()
      inputPath.exists shouldBe false
      rootDir.delete()
    }
    "Convert schema" should "convert all schemas in domain" in {
      createWithWriteDefaultFormat()
      val inputPath = File(rootDir.pathAsString, domainName, schemaName)
      inputPath.exists shouldBe true
      val csvFile = File(outputDir.pathAsString, domainName, schemaName + ".csv")
      csvFile.exists shouldBe false
      new Parquet2CSV(config.copy(schemaName = None), storageHandler).run()
      csvFile.delete()
      rootDir.delete()
    }
    "Convert schema" should "convert all domains and schemas in path" in {
      createWithWriteDefaultFormat()
      val inputPath = File(rootDir.pathAsString, domainName, schemaName)
      inputPath.exists shouldBe true
      val csvFile = File(outputDir.pathAsString, domainName, schemaName + ".csv")
      csvFile.exists shouldBe false
      new Parquet2CSV(config, storageHandler).run()
      csvFile.exists shouldBe true
      csvFile.delete()
      rootDir.delete()
    }

    "All Parquet Config" should "be known and taken  into account" in {
      val rendered = Parquet2CSVCmd.usage()
      println(rendered)
      val expected = {
        """
          |Usage: starlake parquet2csv [options]
          |
          |Convert Parquet files to CSV format for inspection or downstream processing.
          |
          |Convert parquet files to CSV.
          |The folder hierarchy should be in the form /input_folder/domain/schema/part*.parquet
          |Once converted the csv files are put in the folder /output_folder/domain/schema.csv file
          |When the specified number of output partitions is 1 then /output_folder/domain/schema.csv is the file containing the data
          |otherwise, it is a folder containing the part*.csv files.
          |When output_folder is not specified, then the input_folder is used a the base output folder.
          |
          |example: starlake parquet2csv
          |         --input_dir /tmp/datasets/accepted/
          |         --output_dir /tmp/datasets/csv/
          |         --domain sales
          |         --schema orders
          |         --option header=true
          |         --option separator=,
          |         --partitions 1
          |         --write_mode overwrite
          |  --input_dir <value>      Full Path to input directory
          |  --output_dir <value>     Full Path to output directory, if not specified, input_dir is used as output dir
          |  --domain <value>         Domain name to convert. All schemas in this domain are converted. If not specified, all schemas of all domains are converted
          |  --schema <value>         Schema name to convert. If not specified, all schemas are converted.
          |  --delete_source          Should we delete source parquet files after conversion ?
          |  --write_mode <value>     One of Set(OVERWRITE,APPEND)
          |  --options k1=v1,k2=v2...
          |                           Any Spark option to use (sep, delimiter, quote, quoteAll, escape, header ...)
          |  --partitions <value>     How many output partitions
          |  --reportFormat <value>   Report format: console, json, html
          |""".stripMargin
      }
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")
    }
    "Parquet Scalate" should "be generated correctly" in {
      val expected =
        """---
          |sidebar_position: 10
          |title: parquet2csv
          |description: "Convert Parquet files to CSV format with configurable partitions, write modes, and Spark options like delimiter and header."
          |keywords: [starlake parquet2csv, parquet to csv, file conversion, data export, spark]
          |---
          |
          |
          |## Synopsis
          |
          |**starlake parquet2csv [options]**
          |
          |## Description
          |
          |Convert Parquet files to CSV format for inspection or downstream processing.
          |
          |Convert parquet files to CSV.
          |The folder hierarchy should be in the form /input_folder/domain/schema/part*.parquet
          |Once converted the csv files are put in the folder /output_folder/domain/schema.csv file
          |When the specified number of output partitions is 1 then /output_folder/domain/schema.csv is the file containing the data
          |otherwise, it is a folder containing the part*.csv files.
          |When output_folder is not specified, then the input_folder is used a the base output folder.
          |
          |````shell
          |starlake parquet2csv
          |         --input_dir /tmp/datasets/accepted/
          |         --output_dir /tmp/datasets/csv/
          |         --domain sales
          |         --schema orders
          |         --option header=true
          |         --option separator=,
          |         --partitions 1
          |         --write_mode overwrite
          |````
          |
          |
          |## Parameters
          |
          |Parameter|Cardinality|Description
          |---|---|---
          |--delete_source|*Optional*|Should we delete source parquet files after conversion ?
          |--domain `<value>`|*Optional*|Domain name to convert. All schemas in this domain are converted. If not specified, all schemas of all domains are converted
          |--input_dir `<value>`|*Required*|Full Path to input directory
          |--options `k1=v1,k2=v2...`|*Optional*|Any Spark option to use (sep, delimiter, quote, quoteAll, escape, header ...)
          |--output_dir `<value>`|*Optional*|Full Path to output directory, if not specified, input_dir is used as output dir
          |--partitions `<value>`|*Optional*|How many output partitions
          |--reportFormat `<value>`|*Optional*|Report format: console, json, html
          |--schema `<value>`|*Optional*|Schema name to convert. If not specified, all schemas are converted.
          |--write_mode `<value>`|*Optional*|One of Set(OVERWRITE, APPEND)
          |""".stripMargin
      // options sorted by name and flags without a value, as CliConfig.markdown renders them (#2017)
      Parquet2CSVCmd.markdown(1).replaceAll("\\s", "") shouldEqual expected.replaceAll("\\s", "")
    }
  }
}
