package ai.starlake.dbt.jinja

import ai.starlake.dbt.DbtSkip
import ai.starlake.dbt.manifest.DbtRelation
import ai.starlake.dbt.warehouse._
import com.hubspot.jinjava.el.ext.NamedParameter
import com.hubspot.jinjava.interpret.{Context, JinjavaInterpreter}
import com.hubspot.jinjava.lib.fn.MacroFunction
import com.hubspot.jinjava.lib.tag.MacroTag
import com.hubspot.jinjava.objects.date.PyishDate
import com.hubspot.jinjava.objects.serialization.PyishSerializable
import com.hubspot.jinjava.tree.{Node, TagNode}
import jinjava.javax.el.{ELContext, ELResolver}

import java.time.{ZoneOffset, ZonedDateTime}
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** What one render of a model, hook or test collected. After a return() (see [[DbtReturn]]),
  * nothing is collected.
  */
final class DbtRenderState {
  var skip: Option[String] = None
  var error: Option[String] = None
  val vars: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap.empty
  val envVars: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap.empty
  val warnings: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  /** The tables of the statements of the render, by name, for load_result() */
  val results: mutable.Map[String, DbtTable] = mutable.Map.empty
  val reads: mutable.LinkedHashSet[WarehouseRead] = mutable.LinkedHashSet.empty

  private def live: Boolean = !DbtReturn.anyReturned()

  def skipWith(reason: String): Unit = if (live && skip.isEmpty) skip = Some(reason)
  def failWith(message: String): Unit = if (live && error.isEmpty) error = Some(message)
  def warn(message: String): Unit = if (live) warnings += message
  def readVar(name: String, value: String): Unit = if (live) vars.put(name, value)
  def readEnvVar(name: String, value: String): Unit = if (live) envVars.put(name, value)
  def read(what: String, result: String): Unit = if (live) reads += WarehouseRead(what, result)
}

object DbtRenderState {
  private val current = new ThreadLocal[DbtRenderState]

  /** The state of the current render; outside a render (while the macros are defined), a state that
    * nothing reads
    */
  def get: DbtRenderState = Option(current.get).getOrElse(new DbtRenderState)

  def withState[T](state: DbtRenderState)(f: => T): T = {
    val previous = current.get
    current.set(state)
    try f
    finally current.set(previous)
  }
}

/** A macro whose call yields the value given to return(), as dbt's macros do: its body stops at the
  * first top-level node after the return
  */
final class ReturningMacro(
  content: util.List[Node],
  name: String,
  argNamesWithDefaults: util.LinkedHashMap[String, Object],
  localContextScope: Context,
  lineNumber: Int,
  startPosition: Int
) extends MacroFunction(
      ReturningMacro.untilReturn(content),
      name,
      argNamesWithDefaults,
      false,
      localContextScope,
      lineNumber,
      startPosition
    ) {
  override def doEvaluate(
    argMap: util.Map[String, Object],
    kwargMap: util.Map[String, Object],
    varArgs: util.List[Object]
  ): Object = {
    val slot = DbtReturn.enter()
    try {
      val out = super.doEvaluate(argMap, kwargMap, varArgs)
      if (DbtReturn.returned(slot)) DbtReturn.value(slot) else out
    } finally DbtReturn.exit()
  }
}

object ReturningMacro {

  /** The nodes of a macro body, iterated until the call being evaluated returns */
  private def untilReturn(nodes: util.List[Node]): util.List[Node] = new util.AbstractList[Node] {
    override def get(index: Int): Node = nodes.get(index)
    override def size(): Int = nodes.size()
    override def iterator(): util.Iterator[Node] = new util.Iterator[Node] {
      private val it = nodes.iterator()
      override def hasNext: Boolean = it.hasNext && !DbtReturn.innermostReturned()
      override def next(): Node = it.next()
    }
  }
}

/** The macro tag, whose macros are [[ReturningMacro]]s */
final class ReturningMacroTag extends MacroTag {
  override protected def constructMacroFunction(
    tagNode: TagNode,
    interpreter: JinjavaInterpreter,
    name: String,
    argNamesWithDefaults: util.LinkedHashMap[String, Object]
  ): MacroFunction =
    new ReturningMacro(
      tagNode.getChildren,
      name,
      argNamesWithDefaults,
      interpreter.getContext,
      interpreter.getLineNumber,
      interpreter.getPosition
    )
}

/** A dbt builtin written in Scala, called like a macro: positional arguments and kwargs */
final class DbtBuiltin(name: String, context: Context)(
  fn: (List[Object], Map[String, Object]) => Object
) extends MacroFunction(
      util.Collections.emptyList[Node](),
      name,
      new util.LinkedHashMap[String, Object](),
      false,
      context,
      0,
      0
    ) {
  override def doEvaluate(
    argMap: util.Map[String, Object],
    kwargMap: util.Map[String, Object],
    varArgs: util.List[Object]
  ): Object = fn(varArgs.asScala.toList, kwargMap.asScala.toMap)
}

object DbtBuiltin {

  /** Calls a macro as Jinja does, kwargs given as NamedParameters */
  def call(macroFunction: MacroFunction, args: List[Object], kwargs: Map[String, Object]): Object =
    macroFunction.evaluate(
      (args ++ kwargs.map { case (k, v) => new NamedParameter(k, v) }): _*
    )
}

/** `this`, ref() and source(): renders as Starlake's domain.table */
final class DbtJinjaRelation(val relation: DbtRelation, fullDatabase: Option[String])
    extends PyishSerializable {
  def getDatabase: String = fullDatabase.orNull
  def getSchema: String = relation.schema
  def getIdentifier: String = relation.identifier
  def getName: String = relation.identifier
  def getTable: String = relation.identifier
  override def toString: String = relation.render

  /** dbt's Relation is a mapping: `relation.get('metadata', {})`, as dbt_utils._is_relation reads
    */
  def get(key: String, default: Object): Object = key match {
    case "metadata" => util.Map.of("type", "BaseRelation")
    case _          => DbtElResolver.default(default)
  }

  override def appendPyishString[T <: Appendable with CharSequence](appendable: T): T = {
    appendable.append(toString)
    appendable
  }
}

/** `config`: config.get(key, default), config.require(key), and the values config(...) set */
final class DbtConfigObject(initial: util.Map[String, Object]) {
  private val values = new util.LinkedHashMap[String, Object](initial)
  val captured: mutable.LinkedHashMap[String, Object] = mutable.LinkedHashMap.empty

  /** config(...): nothing is set after a return() */
  def set(kwargs: Map[String, Object]): Unit = if (!DbtReturn.anyReturned()) kwargs.foreach {
    case (k, v) =>
      values.put(k, v)
      captured.put(k, v)
  }

  def get(key: String): Object = values.get(key)

  def get(key: String, default: Object): Object =
    Option(values.get(key)).getOrElse(DbtElResolver.default(default))

  def require(key: String): Object =
    Option(values.get(key)).getOrElse {
      DbtRenderState.get.failWith(s"config.require('$key'): '$key' is not set")
      ""
    }
}

/** `api.Column.translate_type`, with the labels of the adapter */
final class DbtColumnApi(adapterType: String) {
  private val labels = adapterType match {
    case "bigquery" =>
      Map("STRING" -> "STRING", "FLOAT" -> "FLOAT64", "INTEGER" -> "INT64", "BOOLEAN" -> "BOOLEAN")
    case _ => Map("STRING" -> "TEXT")
  }
  def translate_type(dtype: String): String = labels.getOrElse(dtype.toUpperCase, dtype)
}

/** `modules.datetime.datetime`: the time of the import */
final class DbtDatetimeModule(at: ZonedDateTime) {
  def now(): PyishDate = new PyishDate(at)
  def utcnow(): PyishDate = new PyishDate(at.withZoneSameInstant(ZoneOffset.UTC))
}

/** `adapter`: the methods that read the warehouse answer from the import's warehouse (all skip
  * without --introspect); the other ones that need it skip the model; quote() and type() answer
  *
  * @param toRelation
  *   a relation from get_relation()'s database, schema and identifier, the database normalized
  * @param toJinja
  *   the relation that the warehouse found, as the templates see it
  */
final class DbtAdapter(
  adapterType: String,
  warehouse: DbtWarehouse,
  toRelation: (Option[String], String, String) => WarehouseRelation,
  toJinja: WarehouseRelation => DbtJinjaRelation
) {
  private val readingMethods = Set("get_relation", "get_columns_in_relation", "get_missing_columns")

  private val warehouseMethods = readingMethods ++ Set(
    "execute",
    "list_relations_without_caching",
    "list_schemas",
    "check_schema_exists",
    "get_column_schema_from_query",
    "load_dataframe",
    "drop_relation",
    "rename_relation",
    "create_schema",
    "drop_schema",
    "already_exists",
    "get_partitions_metadata",
    "describe_relation"
  )

  /** The argument named `name`, else the positional one at `index` */
  private def arg(args: List[Object], index: Int, name: String): Option[Object] =
    args
      .collectFirst { case p: NamedParameter if p.getName == name => p.getValue }
      .orElse(args.filterNot(_.isInstanceOf[NamedParameter]).lift(index))

  def relationOf(o: Object): Option[WarehouseRelation] = o match {
    case r: DbtJinjaRelation =>
      Some(WarehouseRelation(r.relation.database, r.relation.schema, r.relation.identifier))
    case _ => None
  }

  private def answer[T](t: Try[T], fallback: T): T = t match {
    case Success(v) => v
    case Failure(DbtSkip(reason)) =>
      DbtRenderState.get.skipWith(reason)
      fallback
    case Failure(e) =>
      DbtRenderState.get.skipWith(Option(e.getMessage).getOrElse(e.toString))
      fallback
  }

  private def columns(relation: Object): List[DbtWarehouseColumn] = relationOf(relation) match {
    case None => Nil
    case Some(r) =>
      val cols = answer(warehouse.columns(r), Nil)
      DbtRenderState.get.read(s"columns: ${r.render}", s"${cols.size} columns")
      cols
  }

  /** The relation as the warehouse has it, or null (none) */
  def relation(r: WarehouseRelation): Object = {
    val found = answer(warehouse.relation(r), None)
    DbtRenderState.get.read(s"relation: ${r.render}", if (found.isDefined) "found" else "missing")
    found.map(toJinja).orNull
  }

  def invoke(method: String, args: List[Object]): Option[Object] = method match {
    case m if warehouseMethods.contains(m) && (warehouse.offline || !readingMethods.contains(m)) =>
      DbtRenderState.get.skipWith(OfflineWarehouse.reason(s"adapter.$m"))
      Some("")
    case "get_columns_in_relation" =>
      Some(new util.ArrayList[DbtWarehouseColumn](columns(args.headOption.orNull).asJava))
    case "get_relation" =>
      val database =
        arg(args, 0, "database").flatMap(Option(_)).map(String.valueOf).filterNot(_ == "None")
      val schema = String.valueOf(arg(args, 1, "schema").orNull)
      val identifier = String.valueOf(arg(args, 2, "identifier").orNull)
      Some(relation(toRelation(database, schema, identifier)))
    case "get_missing_columns" =>
      val to = columns(args.lift(1).orNull).map(_.getName.toLowerCase).toSet
      val missing = columns(args.headOption.orNull).filterNot(c => to(c.getName.toLowerCase))
      Some(new util.ArrayList[DbtWarehouseColumn](missing.asJava))
    case "quote" =>
      val q = if (adapterType == "bigquery") "`" else "\""
      Some(q + String.valueOf(args.headOption.orNull) + q)
    case "type" => Some(adapterType)
    case _      => None
  }
}

/** `exceptions`: raise_compiler_error skips the model with its message, warn goes to the report */
final class DbtExceptions {
  def invoke(method: String, args: List[Object]): Option[Object] = method match {
    case "raise_compiler_error" | "raise_not_implemented" =>
      DbtRenderState.get.skipWith(String.valueOf(args.headOption.orNull))
      Some("")
    case "warn" =>
      DbtRenderState.get.warn(String.valueOf(args.headOption.orNull))
      Some("")
    case _ => None
  }
}

/** A value that dbt only has with the whole graph or the warehouse, such as `graph` */
final class DbtWarehouseOnly(name: String) {
  def touch(): Unit = DbtRenderState.get.skipWith(s"needs the warehouse at import ($name)")
}

/** Python string and dict methods that Jinjava lacks, the calls of the macros of a package
  * namespace (`dbt_utils.star(...)`), and the dbt objects above
  */
final class DbtElResolver extends ELResolver {
  override def invoke(
    context: ELContext,
    base: Object,
    method: Object,
    paramTypes: Array[Class[_]],
    params: Array[Object]
  ): Object = {
    val name = String.valueOf(method)
    val args = Option(params).map(_.toList).getOrElse(Nil)
    val result: Option[Object] = base match {
      case s: String => DbtElResolver.stringMethod(s, name, args)
      case m: util.Map[_, _] =>
        DbtElResolver.mapMethod(m.asInstanceOf[util.Map[Object, Object]], name, args)
      case a: DbtAdapter       => a.invoke(name, args)
      case e: DbtExceptions    => e.invoke(name, args)
      case w: DbtWarehouseOnly => w.touch(); Some("")
      case _                   => None
    }
    result match {
      case Some(value) =>
        context.setPropertyResolved(true)
        value
      case None => null
    }
  }

  override def getValue(context: ELContext, base: Object, property: Object): Object = base match {
    case w: DbtWarehouseOnly =>
      w.touch()
      context.setPropertyResolved(true)
      new util.LinkedHashMap[String, Object]()
    case t: DbtTable if DbtElResolver.isName(property) =>
      // a list: Jinjava's list resolver does not read its properties
      val value: Option[Object] = String.valueOf(property) match {
        case "columns"      => Some(t.getColumns)
        case "rows"         => Some(t.getRows)
        case "column_names" => Some(t.getColumnNames)
        case _              => None
      }
      value.foreach(_ => context.setPropertyResolved(true))
      value.orNull
    case c: DbtTableColumns if DbtElResolver.isName(property) =>
      // table.columns['status']
      context.setPropertyResolved(true)
      c.byName(String.valueOf(property))
    case _ => null
  }

  override def getType(context: ELContext, base: Object, property: Object): Class[_] = null
  override def setValue(context: ELContext, base: Object, property: Object, value: Object): Unit =
    ()
  override def isReadOnly(context: ELContext, base: Object, property: Object): Boolean = true
  override def getFeatureDescriptors(
    context: ELContext,
    base: Object
  ): util.Iterator[java.beans.FeatureDescriptor] = null
  override def getCommonPropertyType(context: ELContext, base: Object): Class[_] = classOf[Object]
}

object DbtElResolver {

  /** A default given as `default=value` (config.get, dict.get) is its value */
  def default(value: Object): Object = value match {
    case p: NamedParameter if p.getName == "default" => p.getValue
    case other                                       => other
  }

  private def str(o: Object): String = String.valueOf(o)

  /** A property read by name, not a list index */
  def isName(property: Object): Boolean = property match {
    case s: String => !s.forall(c => c.isDigit || c == '-')
    case _         => false
  }

  def stringMethod(s: String, name: String, args: List[Object]): Option[Object] = name match {
    case "upper" => Some(s.toUpperCase)
    case "lower" => Some(s.toLowerCase)
    case "strip" => Some(args.headOption.map(c => stripChars(s, str(c))).getOrElse(s.strip))
    case "lstrip" =>
      Some(args.headOption.map(c => s.dropWhile(str(c).contains(_))).getOrElse(s.stripLeading))
    case "rstrip" =>
      Some(
        args.headOption
          .map(c => s.reverse.dropWhile(str(c).contains(_)).reverse)
          .getOrElse(s.stripTrailing)
      )
    case "startswith" => Some(java.lang.Boolean.valueOf(s.startsWith(str(args.head))))
    case "endswith"   => Some(java.lang.Boolean.valueOf(s.endsWith(str(args.head))))
    case "capitalize" => Some(s.take(1).toUpperCase + s.drop(1).toLowerCase)
    case "title" =>
      Some(s.split(" ", -1).map(w => w.take(1).toUpperCase + w.drop(1).toLowerCase).mkString(" "))
    case "join" =>
      args.headOption.collect { case it: java.lang.Iterable[_] =>
        it.asScala.map(String.valueOf).mkString(s)
      }
    case "format" =>
      Some(
        args.foldLeft(s)((acc, a) =>
          acc.replaceFirst("\\{\\}", java.util.regex.Matcher.quoteReplacement(str(a)))
        )
      )
    case "split" =>
      val parts =
        if (args.isEmpty) s.trim.split("\\s+").toList
        else s.split(java.util.regex.Pattern.quote(str(args.head)), -1).toList
      Some(new util.ArrayList[Object](parts.asJava))
    case _ => None
  }

  private def stripChars(s: String, chars: String): String =
    s.dropWhile(chars.contains(_)).reverse.dropWhile(chars.contains(_)).reverse

  def mapMethod(m: util.Map[Object, Object], name: String, args: List[Object]): Option[Object] =
    (name, args) match {
      case ("get", List(key, default)) =>
        Some(if (m.containsKey(key)) m.get(key) else DbtElResolver.default(default))
      case ("pop", key :: rest) =>
        val had = m.containsKey(key)
        val value = m.remove(key)
        Some(if (had) value else rest.headOption.orNull)
      case _ =>
        m.get(name) match {
          case f: MacroFunction =>
            val (named, positional) = args.partition(_.isInstanceOf[NamedParameter])
            Some(
              DbtBuiltin.call(
                f,
                positional,
                named.collect { case p: NamedParameter => p.getName -> p.getValue }.toMap
              )
            )
          case _ => None
        }
    }
}
