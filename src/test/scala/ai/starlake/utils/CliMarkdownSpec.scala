package ai.starlake.utils

import ai.starlake.TestHelper
import scopt.OParser

/** The Parameters table of a generated CLI page */
class CliMarkdownSpec extends TestHelper {

  private case class SampleConfig(
    domain: String = "",
    force: Boolean = false,
    name: Option[String] = None,
    tags: Seq[String] = Nil,
    after: Option[String] = None
  )

  private object SampleCmd extends CliConfig[SampleConfig] {
    val command = "sample"
    val parser: OParser[Unit, SampleConfig] = {
      val builder = OParser.builder[SampleConfig]
      OParser.sequence(
        builder.programName("starlake sample"),
        builder.head("starlake", "sample", "[options]"),
        builder.note("A sample command."),
        builder
          .arg[String]("domain")
          .required()
          .action((x, c) => c.copy(domain = x))
          .text("Domain name"),
        builder
          .opt[Unit]("force")
          .optional()
          .action((_, c) => c.copy(force = true))
          .text("Overwrite"),
        builder
          .opt[String]("name")
          .optional()
          .action((x, c) => c.copy(name = Some(x)))
          .text("Task name"),
        builder
          .opt[Seq[String]]("tags")
          .valueName("tag1,tag2")
          .unbounded()
          .optional()
          .action((x, c) => c.copy(tags = x))
          .text("Tags"),
        builder
          .opt[String]("after")
          .optional()
          .action((x, c) => c.copy(after = Some(x)))
          .text("Start date")
      )
    }
    def parse(args: Seq[String]): Option[SampleConfig] =
      OParser.parse(parser, args, SampleConfig(), setup)
  }

  new WithSettings() {
    "The Parameters table" should "show a positional argument first and without --, a flag without a value, and the options by name" in {
      val rows = SampleCmd.markdown(1).linesIterator.filter(_.contains("|*")).toList
      rows shouldBe List(
        "`<domain>`|*Required, Positional*|Domain name",
        "--after `<value>`|*Optional*|Start date",
        "--force|*Optional*|Overwrite",
        "--name `<value>`|*Optional*|Task name",
        "--tags `tag1,tag2`|*Optional, Unbounded*|Tags"
      )
    }
  }
}
