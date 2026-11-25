package ai.starlake.dbt.mapping

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.dbt.manifest.{DbtColumn, DbtNode}
import ai.starlake.dbt.report.{EntryStatus, ReportEntry}
import ai.starlake.dbt.jinja.DbtRendered
import ai.starlake.dbt.{DbtSkip, IncrementalDiff}
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

final case class MappedModel(
  task: AutoTaskInfo,
  sql: String,
  entry: ReportEntry,
  vars: Map[String, String],
  envVars: Map[String, String]
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

  def map(node: DbtNode, ctx: DbtMappingContext): Either[ReportEntry, MappedModel] = {
    val relation = ctx.relations.ofNode(node)
    def skip(reason: String) =
      ReportEntry(
        "model",
        node.uniqueId,
        Some(relation.render),
        EntryStatus.Skipped(reason),
        dependsOn = node.dependsOnNodes
      )
    if (node.language != "sql") Left(skip(s"${node.language} model"))
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
            case Success((main, presql, postsql)) =>
              val hooksAndMain = main :: presql ++ postsql
              val split = hooksAndMain.exists(_.split)
              val outside = hooksAndMain.exists(_.outside)
              val renders = hooksAndMain.flatMap(_.renders)
              val (attributes, typeNotes) = attributesOf(node.columns, ctx)
              val notes = strategyNotes ++ configNotes(node) ++ typeNotes ++
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
                  renders.flatMap(_.envVars).toMap
                )
              )
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
        .map(t => s"column ${c.name}: data_type $t has no Starlake type, left out")
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
