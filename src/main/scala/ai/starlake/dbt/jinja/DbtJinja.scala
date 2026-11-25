package ai.starlake.dbt.jinja

import ai.starlake.dbt.manifest._
import ai.starlake.dbt.warehouse._
import ai.starlake.dbt.{DbtImportException, DbtSkip}
import ai.starlake.utils.JinjaUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.hubspot.jinjava.el.JinjavaInterpreterResolver
import com.hubspot.jinjava.interpret.{Context, JinjavaInterpreter, TemplateError}
import com.hubspot.jinjava.lib.fn.{ELFunctionDefinition, MacroFunction}
import com.hubspot.jinjava.objects.date.PyishDate
import com.hubspot.jinjava.{Jinjava, JinjavaConfig, LegacyOverrides}
import jinjava.javax.el.CompositeELResolver

import java.security.MessageDigest
import java.time.ZonedDateTime
import java.util
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** One render: the SQL, or why it failed (a [[DbtSkip]] or an error), and what it read */
final case class DbtRendered(
  sql: Try[String],
  config: Map[String, Object],
  vars: Map[String, String],
  envVars: Map[String, String],
  warnings: List[String],
  warehouseReads: List[WarehouseRead] = Nil
)

/** The values that dbt's context gives every render */
final case class DbtJinjaContext(
  target: DbtTarget,
  vars: DbtVars,
  dispatch: Map[String, List[String]],
  env: String => Option[String],
  now: ZonedDateTime
)

/** A Jinjava instance of its own (Starlake's shared one is untouched) with dbt's context and the
  * macros of the manifest, defined once. Package macros are global, the root project's last so that
  * they win, and each package is also a namespace (`dbt_utils.star`, `dbt.date_trunc`).
  *
  * @param warehouse
  *   what run_query(), statement, load_relation() and the adapter read; offline, they skip the
  *   model
  */
final class DbtJinja(
  manifest: DbtManifest,
  relations: DbtRelations,
  ctx: DbtJinjaContext,
  warehouse: DbtWarehouse = new OfflineWarehouse
) {
  private val adapterType = manifest.metadata.adapterType
  private val root = manifest.metadata.projectName

  private val internalPackages: List[String] =
    List("dbt", s"dbt_$adapterType") ++
    (if (Set("redshift", "materialize").contains(adapterType)) List("dbt_postgres") else Nil)

  private val adapterPrefixes: List[String] =
    List(adapterType) ++ (if (adapterType == "redshift") List("postgres") else Nil) :+ "default"

  val jinjavaConfig: JinjavaConfig = {
    val resolver = new CompositeELResolver()
    resolver.add(new DbtElResolver)
    resolver.add(JinjavaInterpreterResolver.DEFAULT_RESOLVER_READ_WRITE)
    JinjavaConfig
      .newBuilder()
      .withFailOnUnknownTokens(false)
      .withNestedInterpretationEnabled(false)
      .withEnableRecursiveMacroCalls(true)
      .withMaxMacroRecursionDepth(50)
      .withElResolver(resolver)
      .withLegacyOverrides(
        LegacyOverrides
          .newBuilder()
          .withUsePyishObjectMapper(true)
          .withIterateOverMapKeys(true)
          .withUseNaturalOperatorPrecedence(true)
          .build()
      )
      .build()
  }

  private val jinjava = new Jinjava(jinjavaConfig)
  private val json = new ObjectMapper()
  private val global: Context = jinjava.getGlobalContext
  global.registerTag(new ReturningMacroTag)
  global.registerExpTest(new DbtIsMapping)
  global.registerFunction(
    new ELFunctionDefinition("", "return", classOf[DbtReturn], "ret", classOf[Object])
  )

  private val packageOrder: List[String] = {
    val all = manifest.macros.map(_.packageName).distinct
    internalPackages.filter(all.contains) ++
    all.filterNot(p => internalPackages.contains(p) || p == root).sorted ++
    all.filter(_ == root)
  }

  private val definitionErrorsBuffer = scala.collection.mutable.ListBuffer.empty[String]

  /** The macros of each package, by name */
  val macrosByPackage: Map[String, util.Map[String, Object]] = defineMacros()

  /** `dbt`: the core macros and those of the adapter */
  private val dbtNamespace: util.Map[String, Object] = {
    val m = new util.LinkedHashMap[String, Object]()
    internalPackages.flatMap(macrosByPackage.get).foreach(m.putAll)
    m
  }

  packageOrder.foreach { p =>
    macrosByPackage(p)
      .values()
      .asScala
      .foreach(m => global.addGlobalMacro(m.asInstanceOf[MacroFunction]))
  }
  registerBuiltins()

  /** The macros of the manifest that Jinjava could not define, with their error */
  def definitionErrors: List[String] = definitionErrorsBuffer.toList

  private def defineMacros(): Map[String, util.Map[String, Object]] =
    packageOrder.zipWithIndex.map { case (pkg, index) =>
      val context = new Context(global)
      val interpreter = new JinjavaInterpreter(jinjava, context, jinjavaConfig)
      JinjavaInterpreter.pushCurrent(interpreter)
      try {
        manifest.macros
          .filter(_.packageName == pkg)
          .sortBy(_.uniqueId)
          .foreach { m =>
            val before = interpreter.getErrors.size
            Try {
              if (!DbtTemplateText.isMaterialization(m.macroSql)) {
                val rewritten = DbtTemplateText.rewrite(m.macroSql, s"__sl_lit_${index}_")
                rewritten.literals.foreach { case (k, v) => context.put(k, v) }
                interpreter.render(rewritten.text)
              }
            }.failed.foreach(e =>
              definitionErrorsBuffer += s"${m.uniqueId}: ${DbtJinja.message(e)}"
            )
            if (interpreter.getErrors.size > before)
              definitionErrorsBuffer += s"${m.uniqueId}: ${interpreter.getErrors.get(before).getMessage}"
          }
      } finally JinjavaInterpreter.popCurrent()
      pkg -> (new util.LinkedHashMap[String, Object](context.getGlobalMacros): util.Map[
        String,
        Object
      ])
    }.toMap

  private def builtin(name: String, context: Context)(
    fn: (List[Object], Map[String, Object]) => Object
  ): Unit = context.addGlobalMacro(new DbtBuiltin(name, context)(fn))

  /** A database equal to the project's default one is not named, as in [[DbtRelation]] */
  private def normalize(database: Option[String]): Option[String] =
    database.filterNot(d => d.isEmpty || relations.defaultDatabase.exists(_.equalsIgnoreCase(d)))

  private def toJinja(r: WarehouseRelation): DbtJinjaRelation =
    new DbtJinjaRelation(
      DbtRelation(r.database, r.schema, r.identifier),
      r.database.orElse(relations.defaultDatabase)
    )

  private val adapter = new DbtAdapter(
    adapterType,
    warehouse,
    (database, schema, identifier) => WarehouseRelation(normalize(database), schema, identifier),
    toJinja
  )

  private def oneLine(sql: String): String = sql.trim.replaceAll("\\s+", " ")

  private def skipOffline(call: String): Unit =
    DbtRenderState.get.skipWith(OfflineWarehouse.reason(call))

  /** The table of a statement; on a skip, an empty one. `call` names the builtin in the reason of
    * the skip without --introspect.
    */
  private def runQuery(sql: String, call: String): DbtTable = {
    val state = DbtRenderState.get
    if (warehouse.offline) {
      skipOffline(call)
      new DbtTable(Nil, Nil)
    } else
      warehouse.query(sql) match {
        case Success(t) =>
          state.read(s"query: ${oneLine(sql)}", s"${t.size} rows")
          t
        case Failure(DbtSkip(reason)) =>
          state.skipWith(reason)
          new DbtTable(Nil, Nil)
        case Failure(e) =>
          state.skipWith(DbtJinja.message(e))
          new DbtTable(Nil, Nil)
      }
  }

  private def registerBuiltins(): Unit = {
    builtin("run_query", global)((args, _) =>
      runQuery(String.valueOf(args.headOption.orNull), "run_query")
    )
    builtin("statement", global) { (args, kwargs) =>
      if (warehouse.offline) skipOffline("statement")
      else {
        val name = args.headOption.orElse(kwargs.get("name")).map(String.valueOf).getOrElse("main")
        // the body of {% call statement(...) %}: the call tag defines it as the macro `caller`
        val sql = Option(JinjavaInterpreter.getCurrent)
          .flatMap(i => Option(i.getContext.getGlobalMacro("caller")))
          .map(caller => String.valueOf(caller.evaluate()))
          .getOrElse("")
        // after a return() in the body (dbt_utils.get_column_values' default), nothing runs
        if (sql.trim.nonEmpty && !DbtReturn.anyReturned())
          DbtRenderState.get.results.put(name, runQuery(sql, "statement"))
      }
      ""
    }
    builtin("load_result", global) { (args, _) =>
      val found = DbtRenderState.get.results.get(String.valueOf(args.headOption.orNull))
      if (found.isEmpty && warehouse.offline) skipOffline("load_result")
      found.map(DbtLoadResult(_)).orNull
    }
    builtin("load_relation", global) { (args, _) =>
      if (warehouse.offline) {
        skipOffline("load_relation")
        null
      } else adapter.relationOf(args.headOption.orNull).map(adapter.relation).orNull
    }
    List("log", "print", "debug").foreach(name => builtin(name, global)((_, _) => ""))
    builtin("dict", global)((_, kwargs) => new util.LinkedHashMap[String, Object](kwargs.asJava))
    builtin("zip", global) { (args, _) =>
      val lists = args.map(a => a.asInstanceOf[util.Collection[Object]].asScala.toList)
      val n = if (lists.isEmpty) 0 else lists.map(_.size).min
      new util.ArrayList[Object](
        (0 until n).map(i => new util.ArrayList[Object](lists.map(_(i)).asJava): Object).asJava
      )
    }
    builtin("local_md5", global) { (args, _) =>
      MessageDigest
        .getInstance("MD5")
        .digest(String.valueOf(args.head).getBytes("UTF-8"))
        .map("%02x".format(_))
        .mkString
    }
    builtin("tojson", global)((args, _) => json.writeValueAsString(args.head))
    builtin("fromjson", global)((args, _) =>
      json.readValue(String.valueOf(args.head), classOf[Object])
    )
    builtin("__sl_dispatch_macro", global)((args, _) => dispatchTarget(args.head))
    builtin("__sl_dispatch", global) { (args, kwargs) =>
      DbtBuiltin.call(dispatchTarget(args.head), args.tail, kwargs)
    }
  }

  /** adapter.dispatch(name, namespace): `<adapter>__name`, then `default__name`, in the packages of
    * the search order: the dispatch config of dbt_project.yml, else the root project then the
    * namespace
    */
  private def dispatchTarget(spec: Object): MacroFunction = {
    val parts = spec.asInstanceOf[util.List[Object]].asScala.map(String.valueOf).toList
    val name = parts.head
    val namespace = parts.lift(1).filter(p => p != "None" && p.nonEmpty)
    val packages = namespace match {
      case Some(ns) if ctx.dispatch.contains(ns) => ctx.dispatch(ns)
      case Some("dbt")                           => List("dbt")
      case Some(ns) if ns == root                => List(root)
      case Some(ns)                              => List(root, ns)
      case None                                  => List(root, "dbt")
    }
    val found = for {
      pkg    <- packages.iterator
      prefix <- adapterPrefixes.iterator
      ns = if (pkg == "dbt") dbtNamespace else macrosByPackage.getOrElse(pkg, new util.HashMap())
      m <- Option(ns.get(s"${prefix}__$name")).iterator
    } yield m.asInstanceOf[MacroFunction]
    found.nextOption().getOrElse {
      // after a return(), the call has no effect
      if (DbtReturn.anyReturned()) new DbtBuiltin(s"__sl_no_$name", global)((_, _) => "")
      else {
        DbtRenderState.get.failWith(
          s"adapter.dispatch('$name'): no ${adapterPrefixes
              .mkString("__, ")}__ implementation in ${packages.mkString(", ")}"
        )
        throw new DbtImportException(s"adapter.dispatch('$name') has no implementation")
      }
    }
  }

  private def jinjaRelation(relation: DbtRelation, database: Option[String]): DbtJinjaRelation =
    new DbtJinjaRelation(relation, database)

  private def nodeMap(node: DbtNode): Map[String, Any] = Map(
    "unique_id"          -> node.uniqueId,
    "name"               -> node.name,
    "resource_type"      -> node.resourceType,
    "package_name"       -> node.packageName,
    "schema"             -> node.schema,
    "alias"              -> node.alias,
    "database"           -> node.database,
    "original_file_path" -> node.originalFilePath,
    "config"             -> node.config,
    "tags"               -> node.tags
  )

  /** The manifest's graph as Java maps, built once: templates only read it */
  private lazy val graphView: Object = JinjaUtils.toJava(manifest.graph)

  private def staticBindings(): util.Map[String, Object] = {
    val b = new util.HashMap[String, Object]()
    b.put(
      "target",
      JinjaUtils.toJava(
        Map(
          "name"     -> ctx.target.name,
          "type"     -> adapterType,
          "schema"   -> ctx.target.schema,
          "database" -> ctx.target.database,
          "threads"  -> 1
        )
      )
    )
    b.put("execute", java.lang.Boolean.TRUE)
    b.put(
      "flags",
      JinjaUtils.toJava(
        Map("FULL_REFRESH" -> false, "STORE_FAILURES" -> false, "WHICH" -> "compile")
      )
    )
    b.put("api", JinjaUtils.toJava(Map("Column" -> new DbtColumnApi(adapterType))))
    b.put(
      "modules",
      JinjaUtils.toJava(Map("datetime" -> Map("datetime" -> new DbtDatetimeModule(ctx.now))))
    )
    b.put("run_started_at", new PyishDate(ctx.now))
    b.put("adapter", adapter)
    b.put("exceptions", new DbtExceptions)
    b.put("graph", graphView)
    b.put("project_name", root)
    packageOrder.filterNot(internalPackages.contains).foreach(p => b.put(p, macrosByPackage(p)))
    b.put("dbt", dbtNamespace)
    b
  }

  /** Renders a model, hook or test of `node`. `extra` adds bindings (the kwargs of a test). */
  def render(
    node: DbtNode,
    template: String,
    incremental: Boolean,
    extra: Map[String, Object] = Map.empty
  ): DbtRendered = {
    val state = new DbtRenderState
    val dbtConfig =
      Try(
        new DbtConfigObject(JinjaUtils.toJava(node.config).asInstanceOf[util.Map[String, Object]])
      )
    val sql = dbtConfig
      .flatMap(config => Try(renderSql(node, template, incremental, extra, state, config)).flatten)
      .recoverWith {
        case e: DbtSkip            => Failure(e)
        case e: DbtImportException => Failure(e)
        case e                     => Failure(new DbtImportException(DbtJinja.message(e)))
      }
    DbtRendered(
      sql,
      dbtConfig.map(_.captured.toMap).getOrElse(Map.empty),
      state.vars.toMap,
      state.envVars.toMap,
      state.warnings.toList,
      state.reads.toList
    )
  }

  private def renderSql(
    node: DbtNode,
    template: String,
    incremental: Boolean,
    extra: Map[String, Object],
    state: DbtRenderState,
    dbtConfig: DbtConfigObject
  ): Try[String] = {
    val bindings = staticBindings()
    bindings.put("this", jinjaRelation(relations.ofNode(node), node.database))
    bindings.put("model", JinjaUtils.toJava(nodeMap(node)))
    bindings.put("config", dbtConfig)
    extra.foreach { case (k, v) => bindings.put(k, v) }
    val context = new Context(global, bindings)
    builtin("config", context) { (_, kwargs) => dbtConfig.set(kwargs); "" }
    builtin("is_incremental", context)((_, _) => java.lang.Boolean.valueOf(incremental))
    builtin("var", context)((args, kwargs) => varValue(node.packageName, args, kwargs, state))
    builtin("env_var", context)((args, kwargs) => envValue(args, kwargs, state))
    builtin("ref", context)((args, kwargs) => ref(node.packageName, args, kwargs, state))
    builtin("source", context)((args, _) => source(args, state))
    val rewritten = DbtTemplateText.rewrite(template, "__sl_lit_render_")
    rewritten.literals.foreach { case (k, v) => context.put(k, v) }
    val interpreter = new JinjavaInterpreter(jinjava, context, jinjavaConfig)
    val output = DbtRenderState.withState(state) {
      JinjavaInterpreter.pushCurrent(interpreter)
      try Try(interpreter.render(rewritten.text))
      finally JinjavaInterpreter.popCurrent()
    }
    val fatal = interpreter.getErrors.asScala.find(_.getSeverity == TemplateError.ErrorType.FATAL)
    (state.skip, state.error, fatal, output) match {
      case (Some(reason), _, _, _) => Failure(DbtSkip(reason))
      case (_, Some(error), _, _)  => Failure(new DbtImportException(error))
      case (_, _, Some(e), _)      => Failure(new DbtImportException(e.getMessage))
      case (_, _, _, out)          => out
    }
  }

  /** The default of var() and env_var(): the second argument, or `default=` */
  private def defaultArg(args: List[Object], kwargs: Map[String, Object]): Option[Object] =
    args.lift(1).orElse(kwargs.get("default"))

  private def varValue(
    pkg: String,
    args: List[Object],
    kwargs: Map[String, Object],
    state: DbtRenderState
  ): Object = {
    val name = String.valueOf(args.head)
    val found = ctx.vars.lookup(pkg, name)
    found.collect { case DbtUnrenderedVar(reason) => reason }.foreach(state.skipWith)
    found
      .filterNot(_.isInstanceOf[DbtUnrenderedVar])
      .map(JinjaUtils.toJava)
      .orElse(defaultArg(args, kwargs)) match {
      case _ if found.exists(_.isInstanceOf[DbtUnrenderedVar]) => ""
      case Some(value) =>
        state.readVar(name, String.valueOf(value))
        value
      case None =>
        state.skipWith(s"var('$name') has no value and no default")
        ""
    }
  }

  private def envValue(
    args: List[Object],
    kwargs: Map[String, Object],
    state: DbtRenderState
  ): Object = {
    val name = String.valueOf(args.head)
    ctx.env(name).orElse(defaultArg(args, kwargs).map(String.valueOf)) match {
      case _ if name.startsWith("DBT_ENV_SECRET") =>
        // dbt rejects them outside profiles.yml and packages.yml: the secret is not baked
        state.skipWith(
          s"env_var('$name'): a DBT_ENV_SECRET env var is allowed only in profiles.yml and packages.yml"
        )
        ""
      case Some(value) =>
        state.readEnvVar(name, value)
        value
      case None =>
        state.skipWith(s"env_var('$name') is not set and has no default")
        ""
    }
  }

  private def ref(
    callerPackage: String,
    args: List[Object],
    kwargs: Map[String, Object],
    state: DbtRenderState
  ): Object = {
    val names = args.map(String.valueOf)
    val (pkg, name) = names match {
      case List(n)    => (None, n)
      case List(p, n) => (Some(p), n)
      case _ =>
        state.failWith(s"ref(${names.mkString(", ")}): one or two arguments expected")
        (None, "")
    }
    val version = kwargs.get("v").orElse(kwargs.get("version")).map(String.valueOf)
    relations.ref(callerPackage, pkg, name, version) match {
      case Some(target) => jinjaRelation(relations.ofNode(target), target.database)
      case None =>
        state.failWith(s"ref('$name') matches no model, seed or snapshot of the manifest")
        ""
    }
  }

  private def source(args: List[Object], state: DbtRenderState): Object = {
    val names = args.map(String.valueOf)
    relations.source(names.head, names.lift(1).getOrElse("")) match {
      case Some(s) => jinjaRelation(relations.ofSource(s), s.database)
      case None =>
        state.failWith(s"source(${names.mkString(", ")}) matches no source of the manifest")
        ""
    }
  }
}

object DbtJinja {
  private def message(e: Throwable): String = Option(e.getMessage).getOrElse(e.toString)
}
