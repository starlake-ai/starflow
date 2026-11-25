package ai.starlake.dbt.mapping

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.dbt.manifest.DbtNode
import ai.starlake.dbt.report.{EntryStatus, ReportEntry}
import ai.starlake.job.infer.InferSchemaJob
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern
import scala.util.Try

final case class MappedSeed(
  domain: String,
  database: Option[String],
  table: SchemaInfo,
  csv: java.nio.file.Path,
  entry: ReportEntry
)

/** A dbt seed as a load table: metadata/load/<schema>/<alias>.sl.yml, the CSV in the incoming area
  */
object DbtSeedMapper {

  def map(node: DbtNode, ctx: DbtMappingContext): Either[ReportEntry, MappedSeed] = {
    val relation = ctx.relations.ofNode(node)
    def skip(reason: String) =
      ReportEntry("seed", node.uniqueId, Some(relation.render), EntryStatus.Skipped(reason))
    val csv = csvPath(node, ctx)
    if (!Files.isRegularFile(csv)) Left(skip(s"seed file not found: $csv"))
    else
      ctx.inferSeed(csv, relation.identifier).toEither match {
        case Left(e) => Left(skip(s"schema inference failed: ${e.getMessage}"))
        case Right(inferred) =>
          val columnTypes = node.config.get("column_types") match {
            case Some(m: Map[_, _]) =>
              m.asInstanceOf[Map[String, Any]].map { case (k, v) => k -> v.toString }
            case _ => Map.empty[String, String]
          }
          val descriptions = node.columns.map(c => c.name -> c).toMap
          val notes = List.newBuilder[String]
          val attributes = inferred.map { a =>
            val typed = columnTypes.get(a.name) match {
              case Some(t) =>
                DbtTypes.toStarlake(t, ctx.adapterType) match {
                  case Some(starlakeType) => a.copy(`type` = starlakeType)
                  case None =>
                    notes += s"column ${a.name}: column_types $t has no Starlake type, " +
                    "inferred type kept"
                    a
                }
              case None => a
            }
            val comment = descriptions
              .get(a.name)
              .map(c => DbtText.withDocRefs(c.description, c.docBlocks, ctx.manifest))
              .filter(_.nonEmpty)
            typed.copy(comment = comment.orElse(typed.comment), sample = None)
          }
          val separator = node.configString("delimiter").getOrElse(",")
          val table = SchemaInfo(
            name = relation.identifier,
            pattern = Pattern.compile(exactPattern(csv.getFileName.toString)),
            attributes = attributes,
            metadata = Some(
              Metadata(
                format = Some(Format.DSV),
                withHeader = Some(true),
                separator = Some(separator),
                writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)))
              )
            ),
            comment = Some(DbtText.withDocRefs(node.description, node.docBlocks, ctx.manifest))
              .filter(_.nonEmpty),
            tags = node.tags.toSet
          )
          Right(
            MappedSeed(
              relation.schema,
              relation.database,
              table,
              csv,
              ReportEntry(
                "seed",
                node.uniqueId,
                Some(relation.render),
                EntryStatus.Converted,
                notes.result()
              )
            )
          )
      }
  }

  /** A seed of the root project is under the dbt project, a package seed under its package */
  private def csvPath(node: DbtNode, ctx: DbtMappingContext): java.nio.file.Path =
    if (node.packageName == ctx.manifest.metadata.projectName)
      ctx.manifest.projectRoot.resolve(node.originalFilePath)
    else
      node.rootPath
        .map(Paths.get(_).resolve(node.originalFilePath))
        .filter(Files.isRegularFile(_))
        .getOrElse(
          ctx.manifest.projectRoot
            .resolve("dbt_packages")
            .resolve(node.packageName)
            .resolve(node.originalFilePath)
        )

  /** The file name as a regex that matches it only */
  def exactPattern(fileName: String): String =
    fileName.flatMap(c => if (c.isLetterOrDigit || c == '_') c.toString else "\\" + c)

  /** The domain file and the table file of the load tables, then the CSVs */
  def outputs(seeds: List[MappedSeed], connectionRef: Option[String])(implicit
    settings: Settings
  ): List[DbtOutput] =
    seeds.groupBy(_.domain).toList.sortBy(_._1).flatMap { case (domain, tables) =>
      val folder = new Path(DatasetArea.load, domain)
      val domainInfo = DomainInfo(
        name = domain,
        metadata = Some(
          Metadata(
            directory = Some(s"{{SL_DATASETS}}/${settings.appConfig.area.incoming}/$domain"),
            sink = connectionRef.map(c => AllSinks(connectionRef = Some(c)))
          )
        ),
        database = tables.flatMap(_.database).headOption
      )
      FileOutput(
        new Path(folder, "_config.sl.yml"),
        YamlSerde.serialize(DomainDesc(Settings.latestSchemaVersion, domainInfo))
      ) ::
      tables.sortBy(_.table.name).flatMap { s =>
        List(
          FileOutput(
            new Path(folder, s"${s.table.name}.sl.yml"),
            YamlSerde.serialize(TableDesc(Settings.latestSchemaVersion, s.table))
          ),
          CopyOutput(new Path(DatasetArea.incoming(domain), s.csv.getFileName.toString), s.csv)
        )
      }
    }
}

/** Starlake's schema inference (infer-schema) on a seed CSV */
object SeedInference {
  def infer(csv: java.nio.file.Path, tableName: String)(implicit
    settings: Settings
  ): Try[List[TableAttribute]] = {
    implicit val storage: StorageHandler = settings.storageHandler()
    val saveDir = Files.createTempDirectory("dbt-seed-inference")
    new InferSchemaJob()
      .infer(
        domainName = "dbt_seed",
        tableName = tableName,
        pattern = None,
        comment = None,
        inputPath = csv.toAbsolutePath.toString,
        saveDir = saveDir.toString,
        forceFormat = Some(Format.DSV),
        writeMode = WriteMode.OVERWRITE,
        rowTag = None,
        clean = true,
        encoding = StandardCharsets.UTF_8
      )
      .map { tablePath =>
        YamlSerde
          .deserializeYamlTables(storage.read(tablePath), tablePath.toString)
          .head
          .table
          .attributes
      }
  }
}
