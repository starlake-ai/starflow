package ai.starlake.job.bootstrap

import ai.starlake.utils.JarUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.{Codec, Source}

/** Every bootstrap template ships the context an AI assistant reads before it answers anything.
  * They are four separate files because each assistant loads a different one - Claude Code
  * CLAUDE.md, Gemini GEMINI.md and nothing else, Copilot .github/copilot-instructions.md - so a
  * missing one is a silent loss of context, not a visible error.
  */
class BootstrapTemplatesSpec extends AnyFlatSpec with Matchers {

  private val templatesDir = Bootstrap.TEMPLATES_DIR + "/"
  private val contextFiles =
    List("AGENTS.md", "CLAUDE.md", "GEMINI.md", "copilot-instructions.md")

  private def resource(path: String): Option[String] =
    Option(getClass.getClassLoader.getResourceAsStream(path)).map { in =>
      try Source.fromInputStream(in)(Codec.UTF8).mkString
      finally in.close()
    }

  /** The rules block alone: from its heading to the next horizontal rule, or to the end of the
    * file. AGENTS.md continues past it, the three per-assistant files stop there, so slicing to EOF
    * would compare a section against a whole document.
    */
  private def rulesOf(content: String): String = {
    val start = content.indexOf("## Ground rules")
    if (start < 0) ""
    else {
      val body = content.substring(start)
      val end = body.indexOf("\n---")
      (if (end < 0) body else body.substring(0, end)).trim
    }
  }

  private def templatesWithContext: List[String] =
    JarUtil
      .getResourceFolders(templatesDir)
      .filter(t => contextFiles.exists(f => resource(s"$templatesDir$t/$f").isDefined))

  behavior of "the bootstrap templates"

  it should "ship the two documented templates" in {
    templatesWithContext should contain allOf ("empty-project", "sample-project")
  }

  it should "give every assistant the file it actually reads" in {
    templatesWithContext.foreach { template =>
      contextFiles.foreach { file =>
        withClue(s"$template is missing $file: ") {
          resource(s"$templatesDir$template/$file") shouldBe defined
        }
      }
    }
  }

  it should "state the same ground rules in all four, so they cannot drift apart" in {
    templatesWithContext.foreach { template =>
      val rules = contextFiles.map { file =>
        val content = resource(s"$templatesDir$template/$file").getOrElse("")
        withClue(s"$template/$file has no '## Ground rules' section: ") {
          rulesOf(content) should not be empty
        }
        rulesOf(content)
      }
      withClue(s"the ground rules differ between $template's context files: ") {
        rules.distinct should have size 1
      }
    }
  }

  it should "not link to a document it does not ship" in {
    // empty-project's README pointed at a HOW_TO_RUN.md that exists in neither
    // template: a reader follows it, an assistant quotes it, and nothing is there.
    val link = """\[[^\]]*\]\(([^)]+)\)""".r
    templatesWithContext.foreach { template =>
      val shipped = JarUtil
        .getResourceFiles(s"$templatesDir$template/")
        .map(_.substring(s"$templatesDir$template/".length))
        .toSet
      (contextFiles :+ "README.md").foreach { file =>
        resource(s"$templatesDir$template/$file").foreach { content =>
          link
            .findAllMatchIn(content)
            .map(_.group(1))
            .filterNot(t => t.startsWith("http") || t.startsWith("#") || t.startsWith("mailto:"))
            .foreach { target =>
              withClue(s"$template/$file links to $target, which the template does not ship: ") {
                shipped.contains(target.takeWhile(_ != '#')) shouldBe true
              }
            }
        }
      }
    }
  }

  it should "not describe the sample project inside the empty one" in {
    // empty-project's CLAUDE.md and README used to be copies of sample-project's,
    // describing a starbake domain and tables that an empty project never has.
    val docs = contextFiles :+ "README.md"
    docs.foreach { file =>
      resource(s"${templatesDir}empty-project/$file").foreach { content =>
        withClue(s"empty-project/$file mentions the sample project: ") {
          content.toLowerCase should not include "starbake"
        }
      }
    }
  }
}
