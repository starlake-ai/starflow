package ai.starlake.extract

import com.google.cloud.NoCredentials
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.{BigQuery, BigQueryException, BigQueryOptions}
import com.sun.net.httpserver.HttpServer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

/** The real BigQuery client against a local server, so that what is deleted during a scan answers
  * as it does for real: getTable and getDataset return null on a 404, and a listing throws.
  */
class BigQueryInfoSpec extends AnyFlatSpec with Matchers {

  private val project = "/bigquery/v2/projects/p"

  private def datasetJson(dataset: String) =
    s"""{"datasetReference":{"projectId":"p","datasetId":"$dataset"}}"""

  private def tableJson(dataset: String, table: String) =
    s"""{"tableReference":{"projectId":"p","datasetId":"$dataset","tableId":"$table"},"type":"TABLE"}"""

  /** @param datasets
    *   the listing of the project, with the tables that each dataset lists
    * @param answers
    *   what a path answers instead of its content
    */
  private def withBigQuery(datasets: Map[String, List[String]], answers: Map[String, Int] = Map())(
    test: BigQuery => Unit
  ): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      exchange => {
        val path = exchange.getRequestURI.getPath
        val content = path.stripPrefix(project).split("/").toList.filter(_.nonEmpty) match {
          case List("datasets") =>
            val all = datasets.keys.toList.sorted.map(datasetJson).mkString(",")
            s"""{"datasets":[$all]}"""
          case List("datasets", dataset) => datasetJson(dataset)
          case List("datasets", dataset, "tables") =>
            val all = datasets(dataset).map(tableJson(dataset, _)).mkString(",")
            s"""{"tables":[$all]}"""
          case List("datasets", dataset, "tables", table) => tableJson(dataset, table)
          case _                                          => "{}"
        }
        val status = answers.getOrElse(path.stripPrefix(project), 200)
        val body = (if (status == 200) content else s"""{"error":{"code":$status}}""").getBytes
        exchange.getResponseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(status, body.length.toLong)
        exchange.getResponseBody.write(body)
        exchange.close()
      }
    )
    server.start()
    try {
      test(
        BigQueryOptions
          .newBuilder()
          .setProjectId("p")
          .setHost(s"http://127.0.0.1:${server.getAddress.getPort}")
          .setCredentials(NoCredentials.getInstance())
          .setRetrySettings(ServiceOptions.getNoRetrySettings)
          .build()
          .getService
      )
    } finally server.stop(0)
  }

  private def names(bigquery: BigQuery): Map[String, List[String]] =
    BigQueryInfo
      .extractInfo(bigquery)
      .map { case (dataset, tables) =>
        dataset.getDatasetId.getDataset -> tables.map(_.getTableId.getTable)
      }
      .toMap

  "Scanning a project" should "return every dataset and table" in {
    withBigQuery(Map("a" -> List("t1", "t2"), "b" -> List("t3"))) { bigquery =>
      names(bigquery) shouldBe Map("a" -> List("t1", "t2"), "b" -> List("t3"))
    }
  }

  it should "skip a table deleted after the dataset listed it" in {
    withBigQuery(
      Map("a" -> List("gone", "kept")),
      Map("/datasets/a/tables/gone" -> 404)
    ) { bigquery =>
      names(bigquery) shouldBe Map("a" -> List("kept"))
    }
  }

  it should "skip a dataset deleted after the project listed it" in {
    withBigQuery(Map("gone" -> List("t1"), "kept" -> List("t2")), Map("/datasets/gone" -> 404)) {
      bigquery =>
        names(bigquery) shouldBe Map("kept" -> List("t2"))
    }
  }

  it should "skip a dataset deleted before its tables could be listed" in {
    withBigQuery(
      Map("gone" -> List("t1"), "kept" -> List("t2")),
      Map("/datasets/gone/tables" -> 404)
    ) { bigquery =>
      names(bigquery) shouldBe Map("kept" -> List("t2"))
    }
  }

  it should "still fail when it is refused access to a table" in {
    withBigQuery(Map("a" -> List("t1")), Map("/datasets/a/tables/t1" -> 403)) { bigquery =>
      intercept[BigQueryException](names(bigquery)).getCode shouldBe 403
    }
  }

  it should "still fail when it is refused access to the tables of a dataset" in {
    withBigQuery(Map("a" -> List("t1")), Map("/datasets/a/tables" -> 403)) { bigquery =>
      intercept[BigQueryException](names(bigquery)).getCode shouldBe 403
    }
  }
}
