package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.{Cmd, ReportFormatConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

case class IamPoliciesConfig(accessToken: Option[String], reportFormat: Option[String] = None)
    extends ReportFormatConfig

/** Command to apply IAM policies.
  *
  * Usage: starlake iam-policies [options]
  */
object IamPoliciesCmd extends Cmd[IamPoliciesConfig] {

  val command = "iam-policies"

  override def pageDescription: String =
    "Generate and apply IAM policies for your Starflow project resources, managing access control with authentication tokens."
  override def pageKeywords: Seq[String] =
    Seq("starlake iam-policies", "IAM policies", "access control", "security")

  val parser: OParser[Unit, IamPoliciesConfig] = {
    val builder = OParser.builder[IamPoliciesConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Apply IAM (Identity and Access Management) policies defined in Starflow YAML configuration to the target warehouse."""
      ),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[IamPoliciesConfig] =
    OParser.parse(parser, args, IamPoliciesConfig(accessToken = None), setup)

  override def run(config: IamPoliciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).applyIamPolicies(accessToken = None).map(_ => JobResult.empty)
}
