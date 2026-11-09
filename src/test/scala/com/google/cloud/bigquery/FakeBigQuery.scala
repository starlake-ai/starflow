package com.google.cloud.bigquery

import com.google.api.services.bigquery.model.{
  Dataset as DatasetPb,
  DatasetReference,
  ErrorProto,
  Job as JobPb,
  JobConfiguration as JobConfigurationPb,
  JobConfigurationQuery,
  JobReference,
  JobStatistics as JobStatisticsPb,
  JobStatus as JobStatusPb
}
import com.google.cloud.NoCredentials

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** A BigQuery service that answers from a function and records its calls, to run the code of the
  * client without BigQuery. It lives in this package to build the objects of the client.
  */
class FakeBigQuery(answer: PartialFunction[String, AnyRef]) {
  val calls: mutable.Buffer[String] = mutable.Buffer.empty
  private val bigQueryOptions =
    BigQueryOptions
      .newBuilder()
      .setProjectId("project")
      .setCredentials(NoCredentials.getInstance())
      .build()

  val service: BigQuery = Proxy
    .newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[BigQuery]),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          method.getName match {
            case "getOptions" => bigQueryOptions
            case name =>
              calls += name
              answer.applyOrElse(
                name,
                (unexpected: String) => throw new UnsupportedOperationException(unexpected)
              )
          }
      }
    )
    .asInstanceOf[BigQuery]

  /** @param reason
    *   the reason of the error of a job that failed
    * @param childJobs
    *   the statements that a script has run
    */
  def queryJob(
    state: String,
    reason: scala.Option[String] = None,
    childJobs: scala.Option[Long] = None
  ): Job = {
    val status = new JobStatusPb().setState(state)
    reason.foreach(r => status.setErrorResult(new ErrorProto().setReason(r).setMessage(s"Job: $r")))
    val statistics = new JobStatisticsPb()
    childJobs.foreach(n => statistics.setNumChildJobs(n))
    Job.fromPb(
      service,
      new JobPb()
        .setJobReference(new JobReference().setProjectId("project").setJobId("job"))
        .setStatus(status)
        .setStatistics(statistics)
        .setConfiguration(
          new JobConfigurationPb().setQuery(new JobConfigurationQuery().setQuery("SELECT 1"))
        )
    )
  }

  def queryResponse(completed: Boolean): QueryResponse =
    QueryResponse
      .newBuilder()
      .setCompleted(completed)
      .setTotalRows(0L)
      .setErrors(com.google.common.collect.ImmutableList.of[BigQueryError]())
      .build()

  def dataset(description: scala.Option[String], labels: Map[String, String]): Dataset =
    Dataset.fromPb(
      service,
      new DatasetPb()
        .setDatasetReference(new DatasetReference().setProjectId("project").setDatasetId("domain"))
        .setDescription(description.orNull)
        .setLabels(labels.asJava)
    )
}
