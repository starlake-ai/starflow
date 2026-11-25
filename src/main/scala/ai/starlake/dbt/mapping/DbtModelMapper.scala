package ai.starlake.dbt.mapping

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.dbt.manifest.{DbtColumn, DbtNode}
import ai.starlake.dbt.report.{EntryStatus, ReportEntry}
import ai.starlake.dbt.jinja.DbtRendered
import ai.starlake.dbt.warehouse.WarehouseRead
import ai.starlake.dbt.{DbtSkip, IncrementalDiff}
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde
import ai.starlake.sql.{SQLUtils, SqlParser}
import ai.starlake.utils.{IncrementalVariable, Utils}
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

final case class MappedModel(
  task: AutoTaskInfo,
  sql: String,
  entry: ReportEntry,
  vars: Map[String, String],
  envVars: Map[String, String],
  warehouseReads: List[WarehouseRead] = Nil
) {
  def outputs(implicit settings: Settings): List[DbtOutput] = {
    val folder = new Path(DatasetArea.transform, task.domain)
    List(
      FileOutput(
        new Path(folder, s"${task.table}.sl.yml"),
        YamlSerde.serialize(TaskDesc(Settings.latestSchemaVersion, task))
      ),
      FileOutput(new Path(folder, s"${task.table}.sql"), sql + "\n")
    )
  }
}

/** A dbt model as a transform task: metadata/transform/<schema>/<alias>.sl.yml and .sql */
object DbtModelMapper {
  private val TimeDependent = """run_started_at|modules\.datetime""".r
  private val mergeByDefault = Set("bigquery", "snowflake", "databricks", "spark")

  def map(node: DbtNode, ctx: DbtMappingContext)(implicit
    settings: Settings
  ): Either[ReportEntry, MappedModel] = {
    val relation = ctx.relations.ofNode(node)
    def skip(reason: String) =
      ReportEntry(
        "model",
        node.uniqueId,
        Some(relation.render),
        EntryStatus.Skipped(reason),
        dependsOn = node.dependsOnNodes
      )
    val unsafe = DbtText.unsafeField("schema" -> relation.schema, "alias" -> relation.identifier)
    if (unsafe.isDefined) Left(skip(unsafe.get))
    else if (node.language != "sql") Left(skip(s"${node.language} model"))
    else
      writeStrategy(node.materialized, node.config, ctx.adapterType) match {
        case Left(reason) => Left(skip(reason))
        case Right((strategy, strategyNotes)) =>
          val incremental = node.materialized == "incremental"
          val rendered = for {
            sql <- renderBoth(node, node.rawCode, incremental, ctx)
            presql <- Try(
              hooks(node, "pre-hook").map(h => renderBoth(node, h, incremental, ctx).get)
            )
            postsql <- Try(
              hooks(node, "post-hook").map(h => renderBoth(node, h, incremental, ctx).get)
            )
          } yield (sql, presql, postsql)
          rendered match {
            case Failure(DbtSkip(reason)) => Left(skip(reason))
            case Failure(e)               => Left(skip(s"render failed: ${e.getMessage}"))
            case Success((renderedMain, presql, postsql)) =>
              val (main, cteNotes) = renameReservedCtes(renderedMain)
              val hooksAndMain = main :: presql ++ postsql
              val split = hooksAndMain.exists(_.split)
              val outside = hooksAndMain.exists(_.outside)
              val renders = hooksAndMain.flatMap(_.renders)
              val (attributes, typeNotes) = attributesOf(node.columns, ctx)
              val notes = strategyNotes ++ configNotes(node) ++ typeNotes ++ cteNotes ++
                (if (split) List("a string literal holds a Jinja delimiter: split with CONCAT")
                 else Nil) ++
                (if (outside)
                   List(
                     "a Jinja delimiter outside a string literal: Starlake renders it, edit the SQL"
                   )
                 else Nil) ++
                (if (timeDependent(node, ctx))
                   List("renders the time of the import (run_started_at, modules.datetime)")
                 else Nil) ++
                renders.flatMap(_.warnings).distinct.map(w => s"warning: $w")
              val materialization =
                if (Set("view", "ephemeral").contains(node.materialized)) Some(Materialization.VIEW)
                else None
              val sink = AllSinks(
                materializedView = materialization,
                partition = Some(listOf(node.config.get("partition_by"))).filter(_.nonEmpty),
                clustering = Some(listOf(node.config.get("cluster_by"))).filter(_.nonEmpty)
              )
              val task = AutoTaskInfo(
                name = "",
                sql = None,
                database = relation.database,
                domain = relation.schema,
                table = relation.identifier,
                presql = presql.map(_.sql),
                postsql = postsql.map(_.sql),
                sink = if (sink == AllSinks()) None else Some(sink),
                comment = Some(DbtText.withDocRefs(node.description, node.docBlocks, ctx.manifest))
                  .filter(_.nonEmpty),
                attributes = attributes,
                tags = node.tags.toSet,
                writeStrategy = Some(strategy),
                connectionRef = ctx.connectionRef
              )
              Right(
                MappedModel(
                  task,
                  main.sql,
                  ReportEntry(
                    "model",
                    node.uniqueId,
                    Some(relation.render),
                    EntryStatus.Converted,
                    notes,
                    node.dependsOnNodes
                  ),
                  renders.flatMap(_.vars).toMap,
                  renders.flatMap(_.envVars).toMap,
                  renders.flatMap(_.warehouseReads).distinct
                )
              )
          }
      }
  }

  /** Words JSqlParser rejects as a CTE name, common in dbt projects (`final`). DbtModelMapperSpec
    * checks each one against the parser, so the list follows its upgrades.
    */
  val reservedCteNames: List[String] =
    List("final", "window", "output", "pivot", "unpivot", "lateral", "natural")

  /** String literals, quoted identifiers, comments and Jinja tags: copied verbatim */
  private val protectedSpans =
    """'(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`|--[^\n]*|/\*(?s:.*?)\*/|\{%-?(?s:.*?)-?%\}|\{\{(?s:.*?)\}\}|\{#(?s:.*?)#\}""".r

  /** Whether the text parses as Starlake parses a task: with `sl_incremental` false, and true when
    * the text mentions it, after the comments are stripped
    */
  private def parses(text: String)(implicit settings: Settings): Boolean = {
    val renders = false :: (if (IncrementalVariable.mentionedIn(List(text))) List(true) else Nil)
    renders.forall { incremental =>
      Try(
        SqlParser.parseQuietly(
          Utils
            .parseJinja(SQLUtils.stripComments(text), Map(IncrementalVariable.Name -> incremental))
            .trim
        )
      ).isSuccess
    }
  }

  /** `text` with every protected span replaced by spaces */
  private def blanked(text: String): String =
    protectedSpans.replaceAllIn(text, m => " " * m.matched.length)

  /** Renames in `sql` the CTE `name` (definition, `from`/`join` references and qualifiers) to
    * `sl_name`, outside the protected spans
    */
  private def renameCte(sql: String, name: String): String = {
    val n = java.util.regex.Pattern.quote(name)
    val definition = s"(?i)(?<![\\w$$.])$n(?=\\s+as\\s*\\()".r
    val reference = s"(?i)\\b(from|join)(\\s+)$n(?![\\w$$])".r
    val qualifier = s"(?i)(?<![\\w$$.])$n(?=\\s*\\.)".r
    def rename(code: String): String = {
      val d = definition.replaceAllIn(code, _ => s"sl_$name")
      val r = reference.replaceAllIn(d, m => s"${m.group(1)}${m.group(2)}sl_$name")
      qualifier.replaceAllIn(r, _ => s"sl_$name")
    }
    val out = new StringBuilder
    var last = 0
    protectedSpans.findAllMatchIn(sql).foreach { m =>
      out.append(rename(sql.substring(last, m.start))).append(m.matched)
      last = m.end
    }
    out.append(rename(sql.substring(last))).toString
  }

  private val cannotRead =
    "Starlake's SQL parser cannot read this SQL: rewrite it, or set parseSQL: false and make the SQL write the table itself"

  /** The SQL, with the CTEs named like a JSqlParser reserved word renamed when Starlake's parser
    * rejects it, and the notes for the report. The rename is kept only when every render of the SQL
    * (sl_incremental false and true) parses after it.
    */
  private def renameReservedCtes(rendered: Rendered)(implicit
    settings: Settings
  ): (Rendered, List[String]) = {
    val sql = rendered.sql
    if (rendered.outside || rendered.split || parses(sql)) (rendered, Nil)
    else {
      val code = blanked(sql)
      val reserved = reservedCteNames.filter { n =>
        s"(?i)(?<![\\w$$.])${java.util.regex.Pattern.quote(n)}\\s+as\\s*\\(".r
          .findFirstIn(code)
          .isDefined
      }
      // a backslash in a literal may escape a quote (BigQuery, Spark, MySQL): the spans can drift
      val backslashLiteral =
        "'(?:[^']|'')*'".r.findAllIn(sql).exists(_.contains("\\"))
      if (reserved.isEmpty) (rendered, Nil)
      else if (backslashLiteral) (rendered, List(cannotRead))
      else {
        val renamed = reserved.foldLeft(sql)(renameCte)
        if (parses(renamed))
          (
            rendered.copy(sql = renamed),
            reserved.map(n => s"CTE $n renamed sl_$n: $n is reserved for Starlake's SQL parser")
          )
        else (rendered, List(cannotRead))
      }
    }
  }

  /** The SQL of a template, with `sl_incremental` blocks when its two renders differ. Each render
    * is protected (DbtText.protect) before the diff, so that the blocks stay Jinja.
    */
  private def renderBoth(
    node: DbtNode,
    template: String,
    incremental: Boolean,
    ctx: DbtMappingContext
  ): Try[Rendered] = {
    val full = ctx.jinja.render(node, template, incremental = false)
    if (!incremental)
      full.sql.map { sql =>
        val (text, split, outside) = DbtText.protect(sql.trim)
        Rendered(text, split, outside, List(full))
      }
    else {
      val inc = ctx.jinja.render(node, template, incremental = true)
      for { f <- full.sql; i <- inc.sql } yield {
        val (fullText, fullSplit, fullOutside) = DbtText.protect(f.trim)
        val (incText, incSplit, incOutside) = DbtText.protect(i.trim)
        Rendered(
          IncrementalDiff.merge(fullText, incText),
          fullSplit || incSplit,
          fullOutside || incOutside,
          List(full, inc)
        )
      }
    }
  }

  private final case class Rendered(
    sql: String,
    split: Boolean,
    outside: Boolean,
    renders: List[DbtRendered]
  )

  private def hooks(node: DbtNode, key: String): List[String] = node.config.get(key) match {
    case Some(l: List[_]) =>
      l.collect {
        case m: Map[_, _] =>
          m.asInstanceOf[Map[String, Any]].get("sql").map(_.toString).getOrElse("")
        case s: String => s
      }.filter(_.trim.nonEmpty)
    case _ => Nil
  }

  private def listOf(value: Option[Any]): List[String] = value match {
    case Some(s: String) if s.nonEmpty => List(s)
    case Some(l: List[_])              => l.map(_.toString)
    case Some(m: Map[_, _]) => m.asInstanceOf[Map[String, Any]].get("field").map(_.toString).toList
    case _                  => Nil
  }

  /** dbt materialization and incremental strategy as a Starlake write strategy, with its notes */
  def writeStrategy(
    materialized: String,
    config: Map[String, Any],
    adapterType: String
  ): Either[String, (WriteStrategy, List[String])] = {
    val key = listOf(config.get("unique_key"))
    def of(t: WriteStrategyType, notes: List[String] = Nil) =
      Right(
        (
          WriteStrategy(`type` = Some(t), key = if (t == WriteStrategyType.APPEND) Nil else key),
          notes
        )
      )
    materialized match {
      case "table" | "view" => of(WriteStrategyType.OVERWRITE)
      case "ephemeral" =>
        of(
          WriteStrategyType.OVERWRITE,
          List("ephemeral model written as a view: one more object in the warehouse")
        )
      case "incremental" =>
        val strategy = config
          .get("incremental_strategy")
          .collect { case s: String if s.nonEmpty => s }
          .getOrElse {
            if (key.isEmpty) "append"
            else if (mergeByDefault.contains(adapterType)) "merge"
            else "delete+insert"
          }
        strategy match {
          case "append"               => of(WriteStrategyType.APPEND)
          case "merge" if key.isEmpty => of(WriteStrategyType.APPEND)
          case "merge"                => of(WriteStrategyType.UPSERT_BY_KEY)
          case "delete+insert" if key.isEmpty =>
            of(WriteStrategyType.APPEND, List("delete+insert without unique_key appends"))
          case "delete+insert" => of(WriteStrategyType.DELETE_THEN_INSERT)
          case "insert_overwrite" if listOf(config.get("partition_by")).nonEmpty =>
            of(WriteStrategyType.OVERWRITE_BY_PARTITION)
          case "insert_overwrite" =>
            of(
              WriteStrategyType.OVERWRITE,
              List("insert_overwrite without partition_by replaces the whole table")
            )
          case "microbatch" => Left("microbatch incremental strategy")
          case other        => Left(s"incremental strategy '$other' has no Starlake equivalent")
        }
      case other => Left(s"custom materialization '$other'")
    }
  }

  private def configNotes(node: DbtNode): List[String] = {
    val c = node.config
    List(
      c.get("on_schema_change").collect {
        case s: String if s != "ignore" => s"on_schema_change: $s has no equivalent"
      },
      c.get("full_refresh").collect { case false =>
        "full_refresh: false has no equivalent: transform --truncate rebuilds the table"
      },
      c.get("grants").collect { case m: Map[_, _] if m.nonEmpty => "grants are not imported" },
      c.get("contract").collect {
        case m: Map[_, _] if m.asInstanceOf[Map[String, Any]].get("enforced").contains(true) =>
          "contract enforcement is not imported"
      }
    ).flatten
  }

  private def attributesOf(
    columns: List[DbtColumn],
    ctx: DbtMappingContext
  ): (List[TableAttribute], List[String]) = {
    val mapped = columns.map { c =>
      val starlakeType = c.dataType.flatMap(DbtTypes.toStarlake(_, ctx.adapterType))
      val note = c.dataType
        .filter(_ => starlakeType.isEmpty)
        .map(t => s"column ${c.name}: data_type $t has no Starlake type, kept as string")
      val comment =
        Some(DbtText.withDocRefs(c.description, c.docBlocks, ctx.manifest)).filter(_.nonEmpty)
      (
        TableAttribute(name = c.name, `type` = starlakeType.getOrElse("string"), comment = comment),
        note
      )
    }
    (mapped.map(_._1), mapped.flatMap(_._2))
  }

  private def timeDependent(node: DbtNode, ctx: DbtMappingContext): Boolean =
    (node.rawCode :: node.dependsOnMacros.flatMap(ctx.manifest.macrosById.get).map(_.macroSql))
      .exists(TimeDependent.findFirstIn(_).isDefined)
}
