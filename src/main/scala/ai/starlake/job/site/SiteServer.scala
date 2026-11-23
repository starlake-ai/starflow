package ai.starlake.job.site

import better.files.File
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import com.typesafe.scalalogging.LazyLogging

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

/** Serves the generated site folder on 127.0.0.1. Read-only, no directory listing. */
class SiteServer(root: File, port: Int) extends LazyLogging {

  private val rootPath: Path = root.path.toAbsolutePath.normalize()
  private var server: Option[HttpServer] = None
  private var pool: Option[ExecutorService] = None

  private val contentTypes = Map(
    "html"  -> "text/html; charset=utf-8",
    "js"    -> "text/javascript; charset=utf-8",
    "css"   -> "text/css; charset=utf-8",
    "json"  -> "application/json; charset=utf-8",
    "svg"   -> "image/svg+xml",
    "png"   -> "image/png",
    "ico"   -> "image/x-icon",
    "woff2" -> "font/woff2",
    "txt"   -> "text/plain; charset=utf-8"
  )

  private def contentType(p: Path): String = {
    val name = p.getFileName.toString
    val dot = name.lastIndexOf('.')
    if (dot < 0) "application/octet-stream"
    else contentTypes.getOrElse(name.substring(dot + 1).toLowerCase, "application/octet-stream")
  }

  /** Resolves the request path to a regular file strictly inside the root, following symlinks
    * before the containment check. Any malformed input yields None.
    */
  private def resolve(rawPath: String): Option[Path] =
    Try {
      val decoded = URLDecoder.decode(rawPath.replace("+", "%2B"), StandardCharsets.UTF_8)
      val relative = decoded.stripPrefix("/") match {
        case ""    => "index.html"
        case other => other
      }
      val target = rootPath.resolve(relative).normalize()
      if (target.startsWith(rootPath) && Files.isRegularFile(target)) {
        val real = target.toRealPath()
        if (real.startsWith(rootPath.toRealPath())) Some(real) else None
      } else None
    }.toOption.flatten

  private def respond(exchange: HttpExchange, status: Int, body: Array[Byte]): Unit = {
    val isHead = exchange.getRequestMethod == "HEAD"
    exchange.getResponseHeaders.set("X-Content-Type-Options", "nosniff")
    if (isHead) {
      exchange.getResponseHeaders.set("Content-Length", body.length.toString)
      exchange.sendResponseHeaders(status, -1)
    } else {
      exchange.sendResponseHeaders(status, body.length.toLong)
      exchange.getResponseBody.write(body)
    }
  }

  /** Sends a file: the size comes from the file system, the body is streamed, HEAD reads nothing.
    */
  private def respondFile(exchange: HttpExchange, file: Path): Unit = {
    val size = Files.size(file)
    exchange.getResponseHeaders.set("X-Content-Type-Options", "nosniff")
    if (exchange.getRequestMethod == "HEAD") {
      exchange.getResponseHeaders.set("Content-Length", size.toString)
      exchange.sendResponseHeaders(200, -1)
    } else {
      exchange.sendResponseHeaders(200, size)
      Files.copy(file, exchange.getResponseBody)
    }
  }

  private def handle(exchange: HttpExchange): Unit =
    try {
      exchange.getRequestMethod match {
        case "GET" | "HEAD" =>
          resolve(exchange.getRequestURI.getRawPath) match {
            case Some(file) =>
              exchange.getResponseHeaders.set("Content-Type", contentType(file))
              exchange.getResponseHeaders.set("Cache-Control", "no-cache")
              respondFile(exchange, file)
            case None =>
              exchange.getResponseHeaders.set("Content-Type", "text/plain; charset=utf-8")
              respond(exchange, 404, "Not found".getBytes(StandardCharsets.UTF_8))
          }
        case _ =>
          exchange.getResponseHeaders.set("Allow", "GET, HEAD")
          exchange.getResponseHeaders.set("Content-Type", "text/plain; charset=utf-8")
          respond(exchange, 405, "Method not allowed".getBytes(StandardCharsets.UTF_8))
      }
    } catch {
      case e: Exception => logger.warn(s"Request failed: ${e.getMessage}")
    } finally exchange.close()

  def start(): Int = {
    val s = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0)
    s.createContext("/", (e: HttpExchange) => handle(e))
    val bound = s.getAddress.getPort
    val counter = new AtomicInteger(0)
    val executor = Executors.newFixedThreadPool(
      4,
      new ThreadFactory {
        def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"sl-site-server-$bound-${counter.incrementAndGet()}")
          t.setDaemon(true)
          t
        }
      }
    )
    s.setExecutor(executor)
    s.start()
    server = Some(s)
    pool = Some(executor)
    logger.info(s"Serving $rootPath on http://127.0.0.1:$bound/")
    bound
  }

  def stop(): Unit = {
    server.foreach(_.stop(0))
    server = None
    pool.foreach(_.shutdown())
    pool = None
  }
}
