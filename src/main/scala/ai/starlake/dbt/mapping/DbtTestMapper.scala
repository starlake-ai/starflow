package ai.starlake.dbt.mapping

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.dbt.DbtSkip
import ai.starlake.dbt.manifest.DbtNode
import ai.starlake.dbt.report.{EntryStatus, ReportEntry}
import ai.starlake.schema.model.ExpectationItem
import ai.starlake.utils.JinjaUtils
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}

/** @param expectations
  *   by unique id of the model or seed that holds them
  * @param macros
  *   one macro per test that is not one of the four built-in ones
  */
final case class MappedTests(
  expectations: Map[String, List[ExpectationItem]],
  macros: List[String],
  entries: List[ReportEntry]
) {
  def output(implicit settings: Settings): Option[DbtOutput] =
    if (expectations.isEmpty) None
    else
      Some(
        FileOutput(
          new Path(DatasetArea.expectations, "dbt.j2"),
          (DbtTestMapper.BuiltinMacros :: macros).mkString("\n\n") + "\n"
        )
      )
}

/** dbt tests as expectations of the task or table of the tested model, seed or source. 0 = pass. */
object DbtTestMapper {
  val BuiltinMacros: String =
    """{# Written by starlake dbt import: the built-in dbt tests. Each returns the count of failing rows. #}
      |
      |{% macro dbt_not_null(column_name, table='sl_this') -%}
      |SELECT count(*) FROM {{ table }} WHERE {{ column_name }} IS NULL
      |{%- endmacro %}
      |
      |{% macro dbt_unique(column_name, table='sl_this') -%}
      |SELECT count(*) FROM (SELECT {{ column_name }} FROM {{ table }} WHERE {{ column_name }} IS NOT NULL GROUP BY {{ column_name }} HAVING count(*) > 1) AS dbt_unique
      |{%- endmacro %}
      |
      |{% macro dbt_accepted_values(column_name, values, quote=true, table='sl_this') -%}
      |SELECT count(*) FROM {{ table }} WHERE {{ column_name }} NOT IN ({% for v in values %}{% if quote %}'{{ v }}'{% else %}{{ v }}{% endif %}{% if not loop.last %}, {% endif %}{% endfor %})
      |{%- endmacro %}
      |
      |{% macro dbt_relationships(column_name, to, field, table='sl_this') -%}
      |SELECT count(*) FROM {{ table }} WHERE {{ column_name }} IS NOT NULL AND {{ column_name }} NOT IN (SELECT {{ field }} FROM {{ to }} WHERE {{ field }} IS NOT NULL)
      |{%- endmacro %}""".stripMargin

  private val Builtins = Set("not_null", "unique", "accepted_values", "relationships")
  private val TemplateValue = """(?s)^\s*\{\{\s*(.*?)\s*\}\}\s*$""".r
  private val CallValue = """(?s)^\s*((?:ref|source)\(.*\))\s*$""".r
  private val RefArgs = """^\s*ref\(\s*['"]([^'"]+)['"]\s*(?:,\s*['"]([^'"]+)['"]\s*)?\)\s*$""".r
  private val SourceArgs = """^\s*source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*$""".r

  /** @param converted
    *   unique ids of the converted models and seeds
    * @param sourceToSeed
    *   the sources whose table is a seed's: their tests go to the seed
    */
  def map(
    tests: List[DbtNode],
    converted: Set[String],
    sourceToSeed: Map[String, String],
    ctx: DbtMappingContext
  ): MappedTests = {
    val expectations = scala.collection.mutable.LinkedHashMap.empty[String, List[ExpectationItem]]
    val macros = List.newBuilder[String]
    val macroNames = scala.collection.mutable.Set.empty[String]
    val rank = ranks(ctx)
    def seedOf(id: String): String = sourceToSeed.getOrElse(id, id)
    val entries = tests.map { test =>
      def skip(reason: String) =
        ReportEntry("test", test.uniqueId, None, EntryStatus.Skipped(reason))
      val tested = test.attachedNode.orElse(test.dependsOnNodes.lastOption).map(seedOf)
      val reads = (test.dependsOnNodes.map(seedOf) ++ tested).distinct
      tested match {
        case None => skip("no model, seed or source to attach the test to")
        case Some(id) if id.startsWith("source.") =>
          skip(s"tests $id: external tables run no expectations")
        case Some(id) if !converted.contains(id) => skip(s"tests $id, which is not converted")
        case Some(id) if reads.exists(r => !r.startsWith("source.") && !converted.contains(r)) =>
          skip(
            s"reads ${reads.filterNot(r => r.startsWith("source.") || converted.contains(r)).mkString(", ")}, not converted"
          )
        case Some(id) =>
          // Starlake runs an expectation right after its task: it goes to the last built of the
          // tables it reads, which names the tested table when it is another one
          val holder = reads.filter(converted.contains).maxBy(r => (rank(r), r == id))
          def table(node: String) = ctx.relations.ofNode(ctx.manifest.nodesById(node)).render
          val testedTable = if (holder == id) None else Some(table(id))
          // of equal depth, the other tables it reads may be built after the holder
          val sameDepth =
            reads.filter(r => r != holder && converted.contains(r) && rank(r) == rank(holder))
          val failOnError = !test.configString("severity").exists(_.equalsIgnoreCase("warn"))
          val notes = reportedConfig(test) ++
            testedTable
              .map(t => s"runs after ${table(holder)}, the last table it reads, on $t")
              .toList ++
            (if (sameDepth.isEmpty) Nil
             else
               List(
                 s"runs after ${table(holder)}; ${sameDepth.map(table).mkString(", ")} " +
                 s"${if (sameDepth.size == 1) "has" else "have"} the same depth in the dbt graph: " +
                 "no build order between them is guaranteed"
               ))
          val expect = test.testMetadata match {
            case Some(meta) if Builtins.contains(meta.name) && meta.namespace.forall(_ == "dbt") =>
              builtin(test, meta.name, meta.kwargs, testedTable, ctx)
            case _ =>
              rendered(test, ctx).map { sql =>
                val base = s"dbt_test_${DbtText.identifier(test.name)}"
                val name = Iterator
                  .from(1)
                  .map(i => if (i == 1) base else s"${base}_$i")
                  .find(!macroNames.contains(_))
                  .get
                macroNames += name
                macros += s"{% macro $name() -%}\nSELECT count(*) FROM (\n${DbtText.protect(sql.trim)._1}\n) AS dbt_test\n{%- endmacro %}"
                s"$name()"
              }
          }
          expect match {
            case Left(reason) => skip(reason)
            case Right(call) =>
              expectations.put(
                holder,
                expectations.getOrElse(holder, Nil) :+ ExpectationItem(call, failOnError)
              )
              ReportEntry(
                "test",
                test.uniqueId,
                Some(s"$call on ${ctx.relations.ofNode(ctx.manifest.nodesById(holder)).render}"),
                EntryStatus.Converted,
                notes
              )
          }
      }
    }
    MappedTests(expectations.toMap, macros.result(), entries)
  }

  /** The depth of each node in the dbt graph: a node is built after the nodes of lower depth it
    * reads
    */
  private def ranks(ctx: DbtMappingContext): String => Int = {
    val memo = scala.collection.mutable.Map.empty[String, Int]
    def rank(id: String, seen: Set[String]): Int =
      memo.getOrElse(
        id, {
          val deps =
            ctx.manifest.nodesById.get(id).map(_.dependsOnNodes).getOrElse(Nil).filterNot(seen)
          val r = if (deps.isEmpty) 0 else deps.map(rank(_, seen + id)).max + 1
          memo.put(id, r)
          r
        }
      )
    id => rank(id, Set.empty)
  }

  /** @param testedTable
    *   the tested table, when the expectation runs on another task
    */
  private def builtin(
    test: DbtNode,
    name: String,
    kwargs: Map[String, Any],
    testedTable: Option[String],
    ctx: DbtMappingContext
  ): Either[String, String] = {
    val column = DbtText.jinjaLiteral(kwargs.get("column_name").map(_.toString).getOrElse(""))
    val base = testedTable.getOrElse("sl_this")
    val table = test.configString("where") match {
      case Some(where) =>
        ", table=" + DbtText.doubleQuoted(s"(SELECT * FROM $base WHERE $where) AS dbt_where")
      case None => testedTable.map(t => s", table=${DbtText.jinjaLiteral(t)}").getOrElse("")
    }
    name match {
      case "not_null" => Right(s"dbt_not_null($column$table)")
      case "unique"   => Right(s"dbt_unique($column$table)")
      case "accepted_values" =>
        val unquoted = kwargs.get("quote").contains(false)
        // The macro quotes with '{{ v }}' only: the SQL escaping of a quote is done here
        val raw = kwargs.getOrElse("values", Nil) match {
          case l: List[_] if !unquoted =>
            l.map {
              case s: String => s.replace("'", "''")
              case other     => other
            }
          case other => other
        }
        val values = DbtText.jinjaLiteral(raw)
        val quote = if (unquoted) ", quote=false" else ""
        Right(s"dbt_accepted_values($column, $values$quote$table)")
      case "relationships" =>
        val to = kwargs.get("to").map(_.toString).getOrElse("")
        val toRelation = to match {
          case RefArgs(a, b) =>
            val (pkg, model) = Option(b).map(m => (Some(a), m)).getOrElse((None, a))
            ctx.relations
              .ref(test.packageName, pkg, model, None)
              .map(n => ctx.relations.ofNode(n).render)
          case SourceArgs(s, t) =>
            ctx.relations.source(s, t).map(src => ctx.relations.ofSource(src).render)
          case _ => None
        }
        toRelation match {
          case Some(relation) =>
            val field = DbtText.jinjaLiteral(kwargs.get("field").map(_.toString).getOrElse(""))
            Right(s"dbt_relationships($column, ${DbtText.jinjaLiteral(relation)}, $field$table)")
          case None => Left(s"relationships: cannot resolve to: $to")
        }
    }
  }

  /** The SQL of a generic test of a package or of the project, or of a singular test */
  private def rendered(test: DbtNode, ctx: DbtMappingContext): Either[String, String] = {
    val (template, extra) = test.testMetadata match {
      case Some(meta) =>
        val args = meta.kwargs.toList.map {
          case (k, v: String) if TemplateValue.matches(v) =>
            s"$k=${TemplateValue.findFirstMatchIn(v).get.group(1)}"
          case (k, v: String) if CallValue.matches(v) => s"$k=${v.trim}"
          case (k, _)                                 => s"$k=__sl_kw['$k']"
        }
        val prefix = meta.namespace.map(_ + ".").getOrElse("")
        (
          s"{{ ${prefix}test_${meta.name}(${args.mkString(", ")}) }}",
          Map[String, Object]("__sl_kw" -> JinjaUtils.toJava(meta.kwargs))
        )
      case None => (test.rawCode, Map.empty[String, Object])
    }
    ctx.jinja.render(test, template, incremental = false, extra).sql match {
      case Success(sql)             => Right(sql)
      case Failure(DbtSkip(reason)) => Left(reason)
      case Failure(e)               => Left(s"render failed: ${e.getMessage}")
    }
  }

  private def reportedConfig(test: DbtNode): List[String] = {
    val c = test.config
    List(
      c.get("warn_if").collect {
        case s: String if s.replace(" ", "") != "!=0" => s"warn_if: $s is not imported"
      },
      c.get("error_if").collect {
        case s: String if s.replace(" ", "") != "!=0" => s"error_if: $s is not imported"
      },
      c.get("store_failures").collect { case true => "store_failures is not imported" },
      c.get("limit").collect { case n: Number => s"limit: $n is not imported" },
      c.get("fail_calc").collect {
        case s: String if s != "count(*)" => s"fail_calc: $s is not imported"
      }
    ).flatten
  }
}
