package ai.starlake.serve

import ai.starlake.buildinfo.BuildInfo
import ai.starlake.serve.catalog.CatalogCommands
import ai.starlake.utils.Utils
import com.fasterxml.jackson.databind.ObjectMapper
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.ee10.servlet.ServletContextHandler

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import scala.util.{Failure, Success, Try}

object SingleUserMainServer {
  val mapper: ObjectMapper = Utils.newJsonMapper()
  def serve(host: String, port: Int): Try[Unit] = Try {
    val server = new Server(new InetSocketAddress(host, port))
    // Spark 4.3 shades Jetty 12, whose ee10 servlets live in a context, on Jakarta Servlet 6
    val context = new ServletContextHandler()
    context.setContextPath("/")
    context.addServlet(classOf[SingleUserRequestHandler], "/api/v1/cli")
    server.setHandler(context)
    server.start()
    println(s"Server started at $host:$port")
    server.join()
  }

  private var autoReload = true
  def run(
    root: String,
    metadata: Option[String],
    args: Array[String],
    env: Option[String],
    gcpProject: Option[String],
    duckDbMode: Boolean
  ): String = {
    val (settings, reload) =
      CaffeineSettingsManager.getUpdatedSettings("", root, env, duckDbMode, None)
    if (args.head != "quit" && autoReload) {
      SingleUserMainServer.mapper.writeValueAsString(
        SingleUserServices.reset(reload = true)(settings)
      )
    }
    val response = args.head match {
      case "quit" | "exit" =>
        System.exit(0)
        "" // makes the compiler happy
      case "version" => SingleUserMainServer.mapper.writeValueAsString(BuildInfo.version)
      case "reload" =>
        if (args.last == "unset") {
          autoReload = false
          SingleUserMainServer.mapper.writeValueAsString(
            Response("""{"serve":"Auto reload is unset"}""", 0)
          )
        } else {
          if (args.last == "set") {
            autoReload = true
          }
          SingleUserMainServer.mapper.writeValueAsString(
            SingleUserServices.reset(reload)(settings),
            None
          )
        }
      case "heartbeat" => SingleUserMainServer.mapper.writeValueAsString("OK")
      case "domains" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.domains(reload)(settings))
      case "table-names" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.objectNames()(settings))
      case "jobs" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.jobs(reload)(settings))
      case "datawarehouse" =>
        SingleUserMainServer.mapper.writeValueAsString(
          SingleUserServices.targetDatawarehHouse()(settings)
        )
      case "types" =>
        SingleUserMainServer.mapper.writeValueAsString(SingleUserServices.types(reload)(settings))
      case command if CatalogCommands.isCatalogCommand(command) =>
        CatalogCommands.handle(args)(settings)
      case _ =>
        val errCapture = new ByteArrayOutputStream()
        val outCapture = new ByteArrayOutputStream()
        val result =
          Console.withOut(outCapture) {
            Console.withErr(errCapture) {
              SingleUserServices.core(args, reload)(settings)
            }
          }
        if (args.headOption.contains("bootstrap")) {
          CaffeineSettingsManager.remove(root, env)
        }

        result match {
          case Failure(e: IllegalArgumentException) =>
            s"""
               |--------------------------------------------------
               |${errCapture.toString().trim}
               |--------------------------------------------------
               |${e.getMessage}""".stripMargin
          case Failure(exception) =>
            val errMessage = Utils.exceptionAsString(exception)

            SingleUserMainServer.mapper.writeValueAsString(
              Response(errMessage, 1)
            )
          case Success(_) =>
            SingleUserMainServer.mapper.writeValueAsString(
              Response(outCapture.toString.trim, 0)
            )
        }
    }
    response
  }
}

case class Response(serve: String, exitCode: Int = 0)

object Response {
  def apply(output: String, exitCode: Int): Response = {
    val serve =
      output.indexOf(s">>>>>>") match {
        case -1    => output
        case index => output.substring(index + ">>>>>>".length)
      }
    new Response(serve, exitCode)
  }
}
