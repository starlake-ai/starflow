import sbt._
import sbt.io.Using

import java.security.MessageDigest

/** Keeps distrib/python-libs (wheels + versions.txt consumed by Setup.java and `starlake.sh
  * upgrade`) in sync with the template versions pinned in Versions.scala.
  */
object PythonLibs {

  /** PyPI package name -> version pinned in Versions.scala */
  def pinnedPackages: Seq[(String, String)] = Seq(
    "starlake-airflow"       -> Versions.airflowTemplates,
    "starlake-dagster"       -> Versions.dagsterTemplates,
    "starlake-orchestration" -> Versions.orchestrationTemplates,
    "starlake-snowflake"     -> Versions.snowflakeTemplates
  )

  /** PEP 427 wheel file name for a pure-python wheel */
  def wheelName(pkg: String, version: String): String =
    s"${pkg.replace('-', '_')}-$version-py3-none-any.whl"

  def expectedWheels: Seq[String] = pinnedPackages.map { case (pkg, v) => wheelName(pkg, v) }

  /** Offline drift check: fails when versions.txt or the wheels present in `dir` do not match the
    * versions pinned in Versions.scala.
    */
  def check(dir: File, log: Logger): Unit = {
    val versionsTxt = dir / "versions.txt"
    val expected = expectedWheels
    val actual =
      if (versionsTxt.exists())
        IO.readLines(versionsTxt).map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#"))
      else Nil
    val drift = actual.sorted != expected.sorted
    val missingWheels = expected.filterNot(w => (dir / w).exists())
    if (drift) {
      log.error(s"$versionsTxt is out of sync with project/Versions.scala")
      log.error(s"  expected: ${expected.mkString(", ")}")
      log.error(s"  actual  : ${if (actual.isEmpty) "<empty>" else actual.mkString(", ")}")
    }
    missingWheels.foreach(w => log.error(s"Missing wheel: ${dir / w}"))
    if (drift || missingWheels.nonEmpty)
      sys.error(
        "Python libs drift detected: run 'sbt syncPythonLibs' and commit distrib/python-libs"
      )
    log.info(s"Python libs in sync with Versions.scala: ${expected.mkString(", ")}")
  }

  /** Downloads the pinned wheels from PyPI (sha256-verified) into `dir`, removes stale wheels and
    * regenerates versions.txt.
    */
  def sync(dir: File, log: Logger): Unit = {
    IO.createDirectory(dir)
    pinnedPackages.foreach { case (pkg, version) =>
      val wheel = wheelName(pkg, version)
      val target = dir / wheel
      if (target.exists()) {
        log.info(s"$wheel already present, skipping download")
      } else {
        val (wheelUrl, sha256) = resolveWheelUrl(pkg, wheel)
        log.info(s"Downloading $wheelUrl")
        val tmp = dir / (wheel + ".part")
        try {
          Using.urlInputStream(url(wheelUrl))(in => IO.transfer(in, tmp))
          sha256.foreach { expectedSha =>
            val actualSha = sha256Hex(tmp)
            if (actualSha != expectedSha)
              sys.error(s"sha256 mismatch for $wheel: expected $expectedSha, got $actualSha")
          }
          IO.move(tmp, target)
        } finally IO.delete(tmp)
      }
    }
    val expected = expectedWheels
    Option(dir.listFiles())
      .getOrElse(Array.empty[File])
      .filter(f => f.getName.endsWith(".whl") && !expected.contains(f.getName))
      .foreach { stale =>
        log.info(s"Removing stale wheel ${stale.getName}")
        IO.delete(stale)
      }
    IO.write(dir / "versions.txt", expected.mkString("", "\n", "\n"))
    log.info(s"Wrote ${dir / "versions.txt"}")
  }

  /** Resolves the wheel download URL (and its sha256 when advertised) from the PyPI simple index
    * (PEP 503).
    */
  private def resolveWheelUrl(pkg: String, wheel: String): (String, Option[String]) = {
    val indexUrl = s"https://pypi.org/simple/$pkg/"
    val html = Using.urlInputStream(url(indexUrl))(in => IO.readStream(in))
    val href = ("href=\"([^\"#]*/" + java.util.regex.Pattern.quote(
      wheel
    ) + ")(?:#sha256=([0-9a-f]{64}))?\"").r
    href.findFirstMatchIn(html) match {
      case Some(m) => (m.group(1), Option(m.group(2)))
      case None => sys.error(s"$wheel not found on $indexUrl: is this version published on PyPI?")
    }
  }

  private def sha256Hex(file: File): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(IO.readBytes(file))
      .map("%02x".format(_))
      .mkString
}
