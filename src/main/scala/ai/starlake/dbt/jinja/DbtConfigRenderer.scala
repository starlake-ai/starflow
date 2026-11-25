package ai.starlake.dbt.jinja

import com.hubspot.jinjava.interpret.{JinjavaInterpreter, TemplateError}
import com.hubspot.jinjava.lib.filter.Filter
import com.hubspot.jinjava.{Jinjava, JinjavaConfig}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Renders the values of profiles.yml and of the vars of dbt_project.yml that hold Jinja, as dbt
  * does before it reads them: `env_var(name, default)` from the environment, `var(name, default)`
  * from `--vars`. A Jinjava instance of its own: Starlake's shared one is untouched.
  *
  * @param cliVars
  *   the `--vars` of the import
  */
final class DbtConfigRenderer(env: String => Option[String], cliVars: Map[String, Any]) {

  /** The env vars that the rendered values read, in order: a DBT_ENV_SECRET value is masked */
  val envVars: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap.empty

  private val jinjava = new Jinjava(
    JinjavaConfig.newBuilder().withFailOnUnknownTokens(false).build()
  )
  List("as_text", "as_native", "as_bool", "as_number").foreach { name =>
    jinjava.getGlobalContext.registerFilter(new Filter {
      override def getName: String = name
      override def filter(value: Object, interpreter: JinjavaInterpreter, args: String*): Object =
        value
    })
  }

  /** Why the current render fails: the first missing env var or var */
  private var failure: Option[String] = None

  private def default(args: List[Object], kwargs: Map[String, Object]): Option[Object] =
    args.lift(1).orElse(kwargs.get("default"))

  private val global = jinjava.getGlobalContext
  global.addGlobalMacro(new DbtBuiltin("env_var", global)({ (args, kwargs) =>
    val name = String.valueOf(args.head)
    env(name).orElse(default(args, kwargs).map(String.valueOf)) match {
      case Some(value) =>
        envVars.put(name, if (name.startsWith("DBT_ENV_SECRET")) "****" else value)
        value
      case None =>
        if (failure.isEmpty) failure = Some(s"env_var('$name') is not set and has no default")
        ""
    }
  }))
  global.addGlobalMacro(new DbtBuiltin("var", global)({ (args, kwargs) =>
    val name = String.valueOf(args.head)
    cliVars.get(name).map(v => String.valueOf(v): Object).orElse(default(args, kwargs)) match {
      case Some(value) => value
      case None =>
        if (failure.isEmpty) failure = Some(s"var('$name') has no value in --vars and no default")
        ""
    }
  }))

  /** A text without `{{` nor `{%` as it is; otherwise its render, or why it cannot be rendered */
  def render(text: String): Either[String, String] =
    if (!text.contains("{{") && !text.contains("{%")) Right(text)
    else {
      failure = None
      Try(jinjava.renderForResult(text, new java.util.HashMap[String, Object]())).toEither match {
        case Left(e) => Left(Option(e.getMessage).getOrElse(e.toString))
        case Right(result) =>
          val fatal =
            result.getErrors.asScala.find(_.getSeverity == TemplateError.ErrorType.FATAL)
          failure
            .orElse(fatal.map(_.getMessage))
            .map(Left(_))
            .getOrElse(Right(result.getOutput))
      }
    }

  /** A YAML value, its strings rendered: maps and lists through */
  def renderValue(value: Any): Either[String, Any] = value match {
    case s: String => render(s)
    case m: Map[_, _] =>
      val rendered = m.toList.map { case (k, v) => renderValue(v).map(k -> _) }
      rendered.collectFirst { case Left(e) => e }.map(Left(_)).getOrElse {
        Right(scala.collection.immutable.ListMap(rendered.collect { case Right(kv) => kv }: _*))
      }
    case l: List[_] =>
      val rendered = l.map(renderValue)
      rendered.collectFirst { case Left(e) => e }.map(Left(_)).getOrElse {
        Right(rendered.collect { case Right(v) => v })
      }
    case other => Right(other)
  }
}
