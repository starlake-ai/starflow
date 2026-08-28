package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Cmd
import ai.starlake.schema.generator.Xls2Yml.{writeDomainsAsYaml, writeIamPolicyTagsAsYaml}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.IamPolicyTags
import ai.starlake.utils.JobResult
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import scopt.OParser

import scala.util.Try

/** Command to convert XLS to YML.
  *
  * Usage: starlake xls2yml [options]
  */
trait Xls2YmlCmd extends Cmd[Xls2YmlConfig] with LazyLogging {

  def command = "xls2yml"

  override def pageDescription: String =
    "Convert Excel files describing domains, schemas and attributes into Starflow YAML configuration files."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake xls2yml",
      "Excel to YAML",
      "schema generation",
      "data modeling",
      "domain definition"
    )

  val parser: OParser[Unit, Xls2YmlConfig] = {
    val builder = OParser.builder[Xls2YmlConfig]
    OParser.sequence(
      builder.programName(s"starlake $command"),
      builder.head("starlake", command, "[options]"),
      builder.note(
        """Generate Starflow YAML configuration files from Excel spreadsheets."""
      ),
      builder
        .opt[Seq[String]]("files")
        .action { (x, c) =>
          val allFiles = x.flatMap { f =>
            val file = File(f)
            file match {
              case _ if file.isDirectory =>
                file.collectChildren(_.name.endsWith(".xlsx")).toList
              case _ if file.exists => List(file)
              case _ => throw new IllegalArgumentException(s"File $file does not exist")
            }
          }

          c.copy(files = allFiles.map(_.pathAsString))
        }
        .required()
        .text("List of Excel files describing domains & schemas or jobs"),
      builder
        .opt[String]("iamPolicyTagsFile")
        .action((x, c) => c.copy(iamPolicyTagsFile = Some(x)))
        .optional()
        .text("If true generate IAM PolicyTags YML"),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputPath = Some(x)))
        .optional()
        .text(
          """Path for saving the resulting YAML file(s). Starflow domains path is used by default.""".stripMargin
        ),
      builder
        .opt[String]("policyFile")
        .action((x, c) => c.copy(policyFile = Some(x)))
        .optional()
        .text(
          """Optional File for centralising ACL & RLS definition.""".stripMargin
        ),
      builder
        .opt[Boolean]("job")
        .action((x, c) => c.copy(job = x))
        .optional()
        .text("If true generate YML for a Job."),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Xls2YmlConfig] =
    OParser.parse(parser, args, Xls2YmlConfig(), setup)

  override def run(config: Xls2YmlConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try {
      if (config.job) {
        config.files.foreach(
          Xls2YmlAutoJob.generateSchema(_, config.policyFile, config.outputPath)
        )
      } else {
        config.files.foreach { file =>
          logger.info(s"Generating schemas for $file")
          writeDomainsAsYaml(file, config.outputPath)
        }
      }
      config.iamPolicyTagsFile.foreach { iamPolicyTagsPath =>
        val workbook = new XlsIamPolicyTagsReader(InputPath(iamPolicyTagsPath))
        val iamPolicyTags = IamPolicyTags(workbook.iamPolicyTags)
        writeIamPolicyTagsAsYaml(
          iamPolicyTags,
          config.outputPath.getOrElse(DatasetArea.metadata.toString),
          "iam-policy-tags"
        )(settings.storageHandler())
      }
    }.map(_ => JobResult.empty)
  }
}

object Xls2YmlCmd extends Xls2YmlCmd
