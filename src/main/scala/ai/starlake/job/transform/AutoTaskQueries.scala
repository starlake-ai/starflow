package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.common.TaskSQLStatements
import ai.starlake.schema.model.ExpectationSQL

case class AutoTaskQueries(
  statements: TaskSQLStatements,
  expectationItems: List[ExpectationSQL],
  audit: Option[TaskSQLStatements],
  acl: List[String],
  connectionOptions: Map[String, String]
)

object AutoTaskQueries {

  /** @param databricks
    *   the statements run on Databricks, as those of the databricks orchestrator (#1985)
    */
  def apply(task: AutoTask, databricks: Boolean = false)(implicit
    settings: Settings
  ): AutoTaskQueries = {
    AutoTaskQueries(
      task.buildListOfSQLStatements(),
      task.expectationStatements(),
      task.auditStatements(),
      task.aclSQL(databricks = databricks),
      task.buildConnection()
    )
  }

}
