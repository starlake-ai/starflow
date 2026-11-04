package ai.starlake.setup

import com.sun.net.httpserver.HttpServer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.net.InetSocketAddress
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import scala.sys.process._

/** Runs the real download and the real tar of the installer against a local server. Setup is in the
  * default package, so Scala can only reach it through reflection.
  */
class SparkDownloadSpec extends AnyFlatSpec with Matchers {

  private val tarballName = "spark-9.9.9-bin-hadoop3.tgz"

  private lazy val tarball: Array[Byte] = {
    val dir = Files.createTempDirectory("spark-tarball")
    val conf = dir.resolve("spark-9.9.9-bin-hadoop3/conf")
    Files.createDirectories(conf)
    Files.write(conf.resolve("log4j2.properties.template"), "rootLogger.level = warn".getBytes)
    val file = dir.resolve(tarballName)
    Seq("tar", "-czf", file.toString, "-C", dir.toString, "spark-9.9.9-bin-hadoop3").! shouldBe 0
    Files.readAllBytes(file)
  }

  private def sha512(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-512").digest(bytes).map("%02x".format(_)).mkString

  private def setup: Class[_] = Class.forName("Setup")

  private def download(binDir: Path, expectedSha512: String, urls: String*): Unit =
    try {
      setup
        .getMethod("downloadSpark", classOf[File], classOf[String], classOf[Array[String]])
        .invoke(null, binDir.toFile, expectedSha512, urls.toArray)
    } catch { case e: InvocationTargetException => throw e.getCause }

  /** Answers /ours and /apache with the given status, 200 being the tarball. */
  private def withServer(ours: Int, apache: Int = 200)(
    test: (String, String, () => List[String]) => Unit
  ): Unit = {
    val requests = new ConcurrentLinkedQueue[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    def serve(path: String, status: Int): Unit =
      server.createContext(
        path,
        exchange => {
          requests.add(path)
          if (status == 200) {
            exchange.sendResponseHeaders(200, tarball.length.toLong)
            exchange.getResponseBody.write(tarball)
          } else exchange.sendResponseHeaders(status, -1)
          exchange.close()
        }
      )
    serve("/ours", ours)
    serve("/apache", apache)
    server.start()
    val base = s"http://127.0.0.1:${server.getAddress.getPort}"
    try {
      test(
        s"$base/ours/$tarballName",
        s"$base/apache/$tarballName?action=download",
        () => requests.asScala.toList
      )
    } finally server.stop(0)
  }

  private def installed(binDir: Path): Boolean =
    Files.exists(binDir.resolve("spark/conf/log4j2.properties"))

  "The Spark download" should "come from our release, without asking Apache" in {
    withServer(ours = 200) { (ours, apache, requests) =>
      val binDir = Files.createTempDirectory("spark-bin")
      download(binDir, sha512(tarball), ours, apache)
      installed(binDir) shouldBe true
      requests() shouldBe List("/ours")
      Files.exists(binDir.resolve(tarballName)) shouldBe false
    }
  }

  it should "fall back to Apache for a version that we did not publish" in {
    withServer(ours = 404) { (ours, apache, requests) =>
      val binDir = Files.createTempDirectory("spark-bin")
      download(binDir, null, ours, apache)
      installed(binDir) shouldBe true
      requests() shouldBe List("/ours", "/apache")
    }
  }

  it should "check the tarball that came from Apache too" in {
    withServer(ours = 503) { (ours, apache, _) =>
      val binDir = Files.createTempDirectory("spark-bin")
      a[RuntimeException] should be thrownBy download(binDir, "0" * 128, ours, apache)
      installed(binDir) shouldBe false
    }
  }

  it should "refuse a tarball with another checksum, and not keep it" in {
    withServer(ours = 200) { (ours, apache, requests) =>
      val binDir = Files.createTempDirectory("spark-bin")
      val error = intercept[RuntimeException](download(binDir, "0" * 128, ours, apache))
      error.getMessage should include(tarballName)
      error.getMessage should include("SHA-512")
      installed(binDir) shouldBe false
      Files.exists(binDir.resolve("spark")) shouldBe false
      Files.exists(binDir.resolve(tarballName)) shouldBe false
      // A tarball that does not match must not be replaced by another one without anyone noticing
      requests() shouldBe List("/ours")
    }
  }

  it should "try our release before Apache in the installer" in {
    val field = setup.getDeclaredField("SPARK_JAR")
    field.setAccessible(true)
    val dependency = field.get(null)
    val urlsField = dependency.getClass.getDeclaredField("urls")
    urlsField.setAccessible(true)
    val urls = urlsField.get(dependency).asInstanceOf[Array[String]].toList
    val name = urls.head.substring(urls.head.lastIndexOf('/') + 1)
    urls should have size 2
    urls.head should startWith("https://github.com/starlake-ai/starflow/releases/download/spark-")
    urls.head should endWith(s"/$name")
    urls(1) should startWith("https://www.apache.org/dyn/closer.lua/spark/")
    urls(1) should include(s"/$name?")
  }
}
