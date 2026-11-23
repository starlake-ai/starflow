package ai.starlake.job.site

import better.files.File
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Files

class SiteServerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val root = File.newTemporaryDirectory("sl-site-serve-")
  (root / "index.html").overwrite("<html>home</html>")
  (root / "sl-model.json").overwrite("""{"schemaVersion":1}""")
  (root / "assets").createDirectories()
  (root / "assets" / "app.js").overwrite("console.log(1)")
  private val secret = File.newTemporaryFile("sl-secret-", ".txt").overwrite("secret")
  Files.createSymbolicLink((root / "link.txt").path, secret.path)

  private val server = new SiteServer(root, 0)
  private lazy val port = server.start()
  private val client = HttpClient.newHttpClient()

  private def request(method: String, path: String): HttpResponse[String] =
    client.send(
      HttpRequest
        .newBuilder(URI.create(s"http://127.0.0.1:$port$path"))
        .method(method, HttpRequest.BodyPublishers.noBody())
        .build(),
      HttpResponse.BodyHandlers.ofString()
    )

  private def get(path: String): HttpResponse[String] = request("GET", path)

  override def afterAll(): Unit = {
    server.stop()
    root.delete(swallowIOExceptions = true)
    secret.delete(swallowIOExceptions = true)
  }

  "SiteServer" should "serve index.html at /" in {
    val r = get("/")
    r.statusCode() shouldBe 200
    r.body() shouldBe "<html>home</html>"
    r.headers().firstValue("Content-Type").get() should startWith("text/html")
  }

  it should "serve files with their content type" in {
    get("/sl-model.json").headers().firstValue("Content-Type").get() should startWith(
      "application/json"
    )
    get("/assets/app.js").headers().firstValue("Content-Type").get() should startWith(
      "text/javascript"
    )
  }

  it should "return 404 for missing files and for paths outside the root" in {
    get("/nope.html").statusCode() shouldBe 404
    get(s"/../${secret.name}").statusCode() shouldBe 404
    get("/%2e%2e/" + secret.name).statusCode() shouldBe 404
  }

  it should "not serve a symlink that points outside the root" in {
    get("/link.txt").statusCode() shouldBe 404
  }

  it should "send nosniff and only accept GET and HEAD" in {
    get("/").headers().firstValue("X-Content-Type-Options").get() shouldBe "nosniff"
    val head = request("HEAD", "/")
    head.statusCode() shouldBe 200
    head.body() shouldBe ""
    request("POST", "/").statusCode() shouldBe 405
    request("DELETE", "/index.html").statusCode() shouldBe 405
  }

  it should "return 404 on an encoded NUL byte" in {
    get("/%00").statusCode() shouldBe 404
  }
}
