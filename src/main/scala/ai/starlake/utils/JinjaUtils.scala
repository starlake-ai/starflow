package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.utils.Formatter.RichFormatter
import com.hubspot.jinjava.{Jinjava, JinjavaConfig}
import com.hubspot.jinjava.interpret.DeferredValue
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition
import com.hubspot.jinjava.mode.EagerExecutionMode

import scala.jdk.CollectionConverters._

object JinjaUtils {

  /** Recursively convert Scala types to Java types for Jinjava template contexts */
  def toJava(v: Any): Object = v match {
    case null       => null
    case None       => null
    case Some(x)    => toJava(x)
    case s: String  => s
    case b: Boolean => java.lang.Boolean.valueOf(b)
    case i: Int     => java.lang.Integer.valueOf(i)
    case l: Long    => java.lang.Long.valueOf(l)
    case d: Double  => java.lang.Double.valueOf(d)
    case m: Map[_, _] =>
      val jm = new java.util.LinkedHashMap[String, Object]()
      m.foreach { case (k, v) => jm.put(k.toString, toJava(v)) }
      jm
    case l: List[_] => l.map(toJava).asJava
    case s: Set[_]  => s.toList.map(toJava).asJava
    case s: Seq[_]  => s.map(toJava).toList.asJava
    case other      => other.asInstanceOf[Object]
  }

  /** Build a Java Map context from Scala key-value pairs */
  def ctx(entries: (String, Any)*): java.util.Map[String, Object] =
    toJava(entries.toMap).asInstanceOf[java.util.Map[String, Object]]

  /** Eager execution that does not wrap in `{% raw %}` the values that hold Jinja: such a value
    * (`"{{SL_CONNECTION}}"` in an env file) is rendered when the task runs, as with the shared
    * instance
    */
  private object DeferringMode extends EagerExecutionMode {
    override def isPreserveRawTags: Boolean = false
  }

  /** Both instances come from here so that they cannot drift apart */
  private def newJinjava(eager: Boolean)(implicit settings: Settings): Jinjava = {
    val curClassLoader = Thread.currentThread.getContextClassLoader
    val res =
      try {
        Thread.currentThread.setContextClassLoader(this.getClass.getClassLoader)
        val builder = JinjavaConfig
          .newBuilder()
          .withFailOnUnknownTokens(false)
          .withNestedInterpretationEnabled(false)
        val config =
          if (eager) builder.withExecutionMode(DeferringMode).build()
          else builder.build()

        new Jinjava(config)
      } finally Thread.currentThread.setContextClassLoader(curClassLoader)
    res.setResourceLocator(new JinjaResourceHandler())
    res.getGlobalContext.registerFunction(
      new ELFunctionDefinition(
        "",
        "doc",
        classOf[ai.starlake.utils.jinja.DocFunction],
        "doc",
        classOf[String]
      )
    )
    res
  }

  private var _jinjava: Jinjava = null

  def jinjava(implicit settings: Settings): Jinjava = {
    if (_jinjava == null)
      _jinjava = newJinjava(eager = false)
    _jinjava
  }

  private var _deferringJinjava: Jinjava = null

  /** Eager instance: it renders what it can and outputs the tags that depend on a deferred value */
  private def deferringJinjava(implicit settings: Settings): Jinjava = {
    if (_deferringJinjava == null)
      _deferringJinjava = newJinjava(eager = true)
    _deferringJinjava
  }

  def resetJinjaClassLoader(): Unit = {
    _jinjava = null
    _deferringJinjava = null
  }

  /** Renders a task text when the project is loaded, before `sl_incremental` is known: every other
    * variable is rendered, the tags that test or print `sl_incremental` stay in the output, and the
    * task renders them when it runs. A value given for the variable is ignored.
    */
  def parseJinjaDeferringIncremental(str: String, params: Map[String, Any])(implicit
    settings: Settings
  ): String = {
    val others = params - IncrementalVariable.Name
    val context: Map[String, Any] = others + (IncrementalVariable.Name -> DeferredValue.instance())
    deferringJinjava
      .render(str, context.asJava)
      .richFormat(others, Map.empty)
      .trim
  }

  def renderJinja(str: String, params: Map[String, Any] = Map.empty)(implicit
    settings: Settings
  ): String = {
    jinjava.render(str, params.asJava)
  }

  def renderJinja(str: String, params: java.util.Map[String, Object])(implicit
    settings: Settings
  ): String = {
    jinjava.render(str, params)
  }

  def parseJinja(str: String, params: Map[String, Any])(implicit
    settings: Settings
  ): String =
    parseJinja(
      List(str),
      params
    ).head

  def parseJinja(str: List[String], params: Map[String, Any])(implicit
    settings: Settings
  ): List[String] = {
    val result = str.map { sql =>
      renderJinja(sql, params)
        .richFormat(params, Map.empty)
        .trim
    }
    result
  }

  def parseJinjaTpl(
    templateContent: String,
    params: Map[String, Object]
  )(implicit
    settings: Settings
  ): String = {
    parseJinja(templateContent, params)
  }
}
