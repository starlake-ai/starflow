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
    val root = ctx.manifest.projectRoot.toAbsolutePath.normalize
    val csv = csvPath(node, ctx)
    val unsafe = DbtText.unsafeField(
      "schema"   -> relation.schema,
      "alias"    -> relation.identifier,
      "csv file" -> Option(csv.getFileName).map(_.toString).getOrElse("")
    )
    if (unsafe.isDefined) Left(skip(unsafe.get))
    else if (!inside(csv, root))
      Left(skip(s"seed file $csv is outside the dbt project $root"))
    else if (!Files.isRegularFile(csv)) Left(skip(s"seed file not found: $csv"))
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

  private def inside(path: java.nio.file.Path, root: java.nio.file.Path): Boolean =
    path.toAbsolutePath.normalize.startsWith(root)

  /** A seed of the root project is under the dbt project, a package seed under its package: the
    * manifest's root_path of the package when the file is there and in the dbt project, else
    * dbt_packages/<package>. The path is checked to be in the dbt project by the caller.
    */
  private def csvPath(node: DbtNode, ctx: DbtMappingContext): java.nio.file.Path = {
    val root = ctx.manifest.projectRoot.toAbsolutePath.normalize
    if (node.packageName == ctx.manifest.metadata.projectName)
      root.resolve(node.originalFilePath)
    else {
      val fromRootPath = node.rootPath.map(Paths.get(_).resolve(node.originalFilePath))
      val fromPackages =
        root.resolve("dbt_packages").resolve(node.packageName).resolve(node.originalFilePath)
      fromRootPath
        .filter(p => Files.isRegularFile(p) && inside(p, root))
        .orElse(Some(fromPackages).filter(Files.isRegularFile(_)))
        .orElse(fromRootPath.filter(Files.isRegularFile(_)))
        .getOrElse(fromPackages)
    }
  }

  /** The file name as a regex that matches it only */
  def exactPattern(fileName: String): String =
    fileName.flatMap(c => if (c.isLetterOrDigit || c == '_') c.toString else "\\" + c)

  /** The domain file and the table file of the load tables, then the CSVs */
  def outputs(seeds: List[MappedSeed], connectionRef: Option[String])(implicit
    settings: Settings
  ): List[DbtOutput] = plan(seeds, connectionRef)._1

  /** The outputs of the load tables, and the notes of the domain files that are kept. A domain file
    * that exists is not written: when it is not the import's, the seed tables carry the metadata
    * that the domain file would have given them, and their CSVs go to the pending area of the
    * domain, where `starlake load` reads them whatever the directory of the kept domain is.
    */
  def plan(seeds: List[MappedSeed], connectionRef: Option[String])(implicit
    settings: Settings
  ): (List[DbtOutput], List[String]) = {
    val planned = seeds.groupBy(_.domain).toList.sortBy(_._1).map { case (domain, tables) =>
      val folder = new Path(DatasetArea.load, domain)
      val directory = s"{{SL_DATASETS}}/${settings.appConfig.area.incoming}/$domain"
      val sink = connectionRef.map(c => AllSinks(connectionRef = Some(c)))
      val database = tables.flatMap(_.database).headOption
      val domainInfo = DomainInfo(
        name = domain,
        metadata = Some(Metadata(directory = Some(directory), sink = sink)),
        database = database
      )
      val domainFile = DomainFile(
        new Path(folder, "_config.sl.yml"),
        YamlSerde.serialize(DomainDesc(Settings.latestSchemaVersion, domainInfo))
      )
      val kept = domainFile.kept
      val tableOutputs = tables.sortBy(_.table.name).flatMap { s =>
        val table =
          if (!kept) s.table
          else
            s.table.copy(metadata =
              s.table.metadata.map(_.copy(directory = Some(directory), sink = sink))
            )
        val csvFolder = if (kept) DatasetArea.stage(domain) else DatasetArea.incoming(domain)
        List(
          FileOutput(
            new Path(folder, s"${table.name}.sl.yml"),
            YamlSerde.serialize(TableDesc(Settings.latestSchemaVersion, table))
          ),
          CopyOutput(new Path(csvFolder, s.csv.getFileName.toString), s.csv)
        )
      }
      val note = if (kept) {
        val connection = sink.map(_ => ", connectionRef").getOrElse("")
        val db =
          database
            .map(d => s"; database $d is not set: the kept domain file holds it")
            .getOrElse("")
        Some(
          s"${domainFile.path} exists and is kept: the seed tables of $domain carry their own metadata " +
          s"(directory, format, header, separator, write strategy$connection) and their CSVs are copied to " +
          s"${DatasetArea.stage(domain)}, where starlake load reads them$db"
        )
      } else None
      (domainFile.output.toList ++ tableOutputs, note)
    }
    (planned.flatMap(_._1), planned.flatMap(_._2))
  }
}

/** The _config.sl.yml of a domain the import writes. One that exists is never written: [[kept]]
  * when its content is not the import's (it is the user's), skipped silently when it is the same.
  */
final case class DomainFile(path: Path, content: String) {
  private def existing(implicit settings: Settings): Option[String] = {
    val storage = settings.storageHandler()
    if (storage.exists(path)) Some(storage.read(path)) else None
  }

  def kept(implicit settings: Settings): Boolean = existing.exists(_ != content)

  def output(implicit settings: Settings): Option[DbtOutput] =
    if (existing.isDefined) None else Some(FileOutput(path, content))
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
