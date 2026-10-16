package ai.starlake.extract

import ai.starlake.job.Cmd
import ai.starlake.schema.model.WriteMode
import scopt.{OParser, OParserBuilder}

trait TablesExtractCmd extends Cmd[TablesExtractConfig] {

  private def buildTablesMap(tables: Seq[String]): Map[String, List[String]] = {
    val tablesByDomain: Map[String, Seq[(String, String)]] =
      tables
        .map { _.split('.') }
        .map { tab => if (tab.length == 1) tab(0) -> "*" else tab(0) -> tab(1) }
        .groupBy(_._1) // by domain

    tablesByDomain.map { case (k, v) =>
      (k, v.map(_._2).toList)
    }
  }

  /** Options specific to a command, appended to the common ones */
  protected def extraOptions(
    builder: OParserBuilder[TablesExtractConfig]
  ): List[OParser[_, TablesExtractConfig]] = Nil

  val parser: OParser[Unit, TablesExtractConfig] = {
    val builder = OParser.builder[TablesExtractConfig]
    val options = List[OParser[_, TablesExtractConfig]](
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("write")
        .action((x, c) => c.copy(writeMode = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connectionRef = Some(x)))
        .text(s"Connection to use")
        .optional(),
      builder
        .opt[String]("database")
        .action((x, c) => c.copy(database = Some(x)))
        .text(s"database / project id")
        .optional(),
      builder
        .opt[Unit]("external")
        .action((_, c) => c.copy(external = true))
        .text(
          "Include external datasets defined in _config.sl.yml instead of using other parameters of this command ? Defaults to false"
        )
        .optional(),
      builder
        .opt[Seq[String]]("tables")
        .action { (x, c) =>
          c.copy(tables = buildTablesMap(x))
        }
        .optional()
        .text("List of datasetName.tableName1,datasetName.tableName2 ..."),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[Boolean]("persist")
        .action { (x, c) => c.copy(persist = x) }
        .optional()
        .text("Persist results ?")
    ) ++ extraOptions(builder) :+
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    OParser.sequence(builder.programName(s"$shell $command"), options: _*)
  }

  def parse(args: Seq[String]): Option[TablesExtractConfig] =
    OParser.parse(parser, args, TablesExtractConfig(), setup)
}
