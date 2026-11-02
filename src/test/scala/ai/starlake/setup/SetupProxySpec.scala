package ai.starlake.setup

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{BufferedReader, InputStreamReader}
import java.net.ServerSocket
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.io.Source
import scala.jdk.CollectionConverters._

/** Setup resolves the latest release while its class loads, before main configures the HTTP client.
  * Without SL_VERSION, a lookup that ignores the proxy times out on a network that needs one, and
  * the installer then tried to download releases/download/vnull.
  *
  * Runs the real installer in its own JVM against a local proxy that refuses every tunnel.
  */
class SetupProxySpec extends AnyFlatSpec with Matchers {

  private case class Run(requests: List[String], exitCode: Int, output: String)

  private def runSetup(): Run = {
    val requests = new ConcurrentLinkedQueue[String]()
    val proxy = new ServerSocket(0)
    val server = new Thread(() => {
      try {
        while (true) {
          val socket = proxy.accept()
          try {
            val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
            requests.add(in.readLine())
            socket.getOutputStream.write("HTTP/1.1 502 Bad Gateway\r\n\r\n".getBytes)
            socket.getOutputStream.flush()
          } finally socket.close()
        }
      } catch { case _: java.net.SocketException => () }
    })
    server.setDaemon(true)
    server.start()

    val target = Files.createTempDirectory("setup-proxy-spec")
    val classes = Paths.get(
      Class
        .forName("Setup", false, getClass.getClassLoader)
        .getProtectionDomain
        .getCodeSource
        .getLocation
        .toURI
    )
    val javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    val builder =
      new ProcessBuilder(javaBin, "-cp", classes.toString, "Setup", target.toString, "unix")
        .redirectErrorStream(true)
    val env = builder.environment()
    Seq("SL_VERSION", "SL_API_VERSION", "no_proxy", "NO_PROXY").foreach(env.remove)
    val url = s"http://127.0.0.1:${proxy.getLocalPort}"
    env.put("https_proxy", url)
    env.put("http_proxy", url)

    val process = builder.start()
    val output = new StringBuilder
    val reader = new Thread(() =>
      output.append(Source.fromInputStream(process.getInputStream).mkString)
    )
    reader.start()
    try {
      withClue(s"Setup did not stop in time. Output:\n$output\n") {
        process.waitFor(60, TimeUnit.SECONDS) shouldBe true
      }
      reader.join(5000)
      Run(requests.asScala.toList, process.exitValue(), output.toString)
    } finally {
      process.destroyForcibly()
      proxy.close()
    }
  }

  private lazy val run = runSetup()

  "The latest release lookup" should "go through the proxy" in {
    run.requests.head should startWith("CONNECT api.github.com:443")
  }

  "A failed lookup" should "stop the installer with an error naming SL_VERSION" in {
    run.exitCode should not be 0
    run.output should include("SL_VERSION")
  }

  it should "not try to download a version named null" in {
    run.output should not include "vnull"
    run.output should not include "-null"
    run.requests.tail shouldBe empty
  }
}
