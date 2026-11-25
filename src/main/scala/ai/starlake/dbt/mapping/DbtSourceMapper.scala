package ai.starlake.dbt.mapping

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.dbt.manifest.{DbtPeriod, DbtSource}
import ai.starlake.dbt.report.{EntryStatus, ReportEntry}
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern

/** @param seedId
  *   the seed whose load table is this source's table: no external table is written for it
  */
final case class MappedSource(
  domain: String,
  database: Option[String],
  table: Option[SchemaInfo],
  seedId: Option[String],
  entry: ReportEntry
)

/** A dbt source as an external table: metadata/external/<schema>/<identifier>.sl.yml */
object DbtSourceMapper {

  /** {count: 12, period: hour} -> "12 hours", as Freshness reads it (scala Duration) */
  def duration(p: DbtPeriod): String = s"${p.count} ${p.period}${if (p.count == 1) "" else "s"}"

  def map(source: DbtSource, ctx: DbtMappingContext): Either[ReportEntry, MappedSource] = {
    val relation = ctx.relations.ofSource(source)
    val unsafe =
      DbtText.unsafeField("schema" -> relation.schema, "identifier" -> relation.identifier)
    if (unsafe.isDefined)
      Left(
        ReportEntry(
          "source",
          source.uniqueId,
          Some(relation.render),
          EntryStatus.Skipped(unsafe.get)
        )
      )
    else if (!source.enabled)
      Left(
        ReportEntry(
          "source",
          source.uniqueId,
          Some(relation.render),
          EntryStatus.NotImported("enabled: false")
        )
      )
    else {
      val seed = ctx.manifest.nodesOfType("seed").find(s => ctx.relations.ofNode(s) == relation)
      seed match {
        case Some(s) =>
          Right(
            MappedSource(
              relation.schema,
              relation.database,
              None,
              Some(s.uniqueId),
              ReportEntry(
                "source",
                source.uniqueId,
                Some(relation.render),
                EntryStatus.Converted,
                List(s"the load table of seed ${s.uniqueId} declares this table"),
                dependsOn = List(s.uniqueId)
              )
            )
          )
        case None =>
          val notes = List.newBuilder[String]
          val attributes = source.columns.map { c =>
            val starlakeType = c.dataType.flatMap(DbtTypes.toStarlake(_, ctx.adapterType))
            c.dataType.filter(_ => starlakeType.isEmpty).foreach { t =>
              notes += s"column ${c.name}: data_type $t has no Starlake type, kept as string"
            }
            TableAttribute(
              name = c.name,
              `type` = starlakeType.getOrElse("string"),
              comment = Some(DbtText.withDocRefs(c.description, c.docBlocks, ctx.manifest))
                .filter(_.nonEmpty)
            )
          }
          source.loadedAtField.foreach { field =>
            notes += s"loaded_at_field $field is not used: " +
            "Starlake's freshness reads the last modification time of the table"
          }
          val freshness = source.freshness.map(f =>
            Freshness(f.warnAfter.map(duration), f.errorAfter.map(duration))
          )
          if (freshness.isDefined)
            notes += "starlake freshness checks load tables and tasks, not external tables"
          val table = SchemaInfo(
            name = relation.identifier,
            pattern = Pattern.compile(DbtSeedMapper.exactPattern(relation.identifier)),
            attributes = attributes,
            metadata = freshness.map(f => Metadata(freshness = Some(f))),
            comment = Some(DbtText.withDocRefs(source.description, source.docBlocks, ctx.manifest))
              .filter(_.nonEmpty),
            tags = source.tags.toSet
          )
          Right(
            MappedSource(
              relation.schema,
              relation.database,
              Some(table),
              None,
              ReportEntry(
                "source",
                source.uniqueId,
                Some(relation.render),
                EntryStatus.Converted,
                notes.result()
              )
            )
          )
      }
    }
  }

  def outputs(sources: List[MappedSource])(implicit settings: Settings): List[DbtOutput] =
    plan(sources)._1

  /** The outputs of the external tables, and the notes of the domain files that are kept: a domain
    * file that exists is not written (see [[DomainFile]])
    */
  def plan(sources: List[MappedSource])(implicit
    settings: Settings
  ): (List[DbtOutput], List[String]) = {
    val planned =
      sources.filter(_.table.isDefined).groupBy(_.domain).toList.sortBy(_._1).map {
        case (domain, tables) =>
          val folder = new Path(DatasetArea.external, domain)
          val database = tables.flatMap(_.database).headOption
          val domainInfo = DomainInfo(name = domain, database = database)
          val domainFile = DomainFile(
            new Path(folder, "_config.sl.yml"),
            YamlSerde.serialize(DomainDesc(Settings.latestSchemaVersion, domainInfo))
          )
          val note =
            if (!domainFile.kept) None
            else
              Some(
                s"${domainFile.path} exists and is kept" +
                database
                  .map(d => s"; database $d is not set: the kept domain file holds it")
                  .getOrElse("")
              )
          val tableOutputs = tables.flatMap(_.table).sortBy(_.name).map { t =>
            FileOutput(
              new Path(folder, s"${t.name}.sl.yml"),
              YamlSerde.serialize(TableDesc(Settings.latestSchemaVersion, t))
            )
          }
          (domainFile.output.toList ++ tableOutputs, note)
      }
    (planned.flatMap(_._1), planned.flatMap(_._2))
  }
}
