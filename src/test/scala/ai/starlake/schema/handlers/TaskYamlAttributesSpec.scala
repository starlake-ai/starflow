package ai.starlake.schema.handlers

import ai.starlake.schema.model.TableAttribute
import ai.starlake.utils.YamlSerde
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The attributes of a task are updated in its YAML file as written (#1953) */
class TaskYamlAttributesSpec extends AnyFlatSpec with Matchers {

  private val path = "transform/d/t.sl.yml"

  /** Renders the variable of the files below and trims the result, as the loading of the task does
    * (`Utils.parseJinja`)
    */
  private def render(content: String): String =
    content.replace("{{ reader_sa }}", "user:someone@example.com").trim

  private def attributesOf(content: String): List[TableAttribute] =
    YamlSerde.deserializeYamlTask(render(content), path).attributes

  private val written =
    """# Header comment: kept by hand, explains the task.
      |version: 1
      |task:
      |  acl:
      |    - role: "roles/bigquery.dataViewer"
      |      grants:
      |        - "{{ reader_sa }}"
      |  attributes:
      |    # The key of the table
      |    - name: id
      |      type: integer
      |      comment: Hand-written description # kept
      |    - name: obsolete
      |    - name: amount
      |      type: long
      |  writeStrategy:
      |    type: OVERWRITE
      |# Trailing comment
      |""".stripMargin

  "replace" should "keep every line outside the attributes, and the items of the attributes that do not change" in {
    val List(id, _, amount) = attributesOf(written)
    val updated = TaskYamlAttributes.replace(
      written,
      List(
        id,
        amount.copy(`type` = "decimal"),
        TableAttribute(name = "month_start", `type` = "timestamp")
      ),
      render,
      path
    )
    updated shouldBe
    """# Header comment: kept by hand, explains the task.
      |version: 1
      |task:
      |  acl:
      |    - role: "roles/bigquery.dataViewer"
      |      grants:
      |        - "{{ reader_sa }}"
      |  attributes:
      |    # The key of the table
      |    - name: id
      |      type: integer
      |      comment: Hand-written description # kept
      |    - name: "amount"
      |      type: "decimal"
      |    - name: "month_start"
      |      type: "timestamp"
      |  writeStrategy:
      |    type: OVERWRITE
      |# Trailing comment
      |""".stripMargin
  }

  it should "follow the order of the new attributes, in a list at the level of its key" in {
    val content =
      """version: 1
        |task:
        |  attributes:
        |  - name: b
        |    comment: second
        |  - name: a
        |  sink:
        |    connectionRef: duckdb
        |""".stripMargin
    val List(b, a) = attributesOf(content)
    TaskYamlAttributes.replace(content, List(a, b), render, path) shouldBe
    """version: 1
      |task:
      |  attributes:
      |  - name: a
      |  - name: b
      |    comment: second
      |  sink:
      |    connectionRef: duckdb
      |""".stripMargin
  }

  it should "add the attributes to a task that has none, in the layout without the task key" in {
    // As serialized by the earlier versions of the sync, and by the tests
    val content =
      """---
        |domain: "d"
        |writeStrategy:
        |  type: "OVERWRITE"
        |""".stripMargin
    TaskYamlAttributes.replace(
      content,
      List(TableAttribute(name = "id", `type` = "integer")),
      render,
      path
    ) shouldBe
    """---
      |attributes:
      |  - name: "id"
      |    type: "integer"
      |domain: "d"
      |writeStrategy:
      |  type: "OVERWRITE"
      |""".stripMargin
  }

  it should "add the attributes first in a task that has none, since the rendering trims a block scalar that ends the file" in {
    val content =
      """version: 1
        |task:
        |  # Inline SQL
        |  sql: |
        |    SELECT 1 AS id
        |""".stripMargin
    TaskYamlAttributes.replace(
      content,
      List(TableAttribute(name = "id", `type` = "integer")),
      render,
      path
    ) shouldBe
    """version: 1
      |task:
      |  attributes:
      |    - name: "id"
      |      type: "integer"
      |  # Inline SQL
      |  sql: |
      |    SELECT 1 AS id
      |""".stripMargin
  }

  it should "move the comments above an item with it, and drop them with it" in {
    val content =
      """version: 1
        |task:
        |  attributes:
        |    # Columns
        |    - name: obsolete
        |      type: string
        |      # Kept with obsolete
        |
        |    # Amount in cents
        |    - name: amount
        |      type: long
        |    # The key
        |    - name: id
        |  domain: d
        |""".stripMargin
    val List(_, amount, id) = attributesOf(content)
    TaskYamlAttributes.replace(content, List(id, amount), render, path) shouldBe
    """version: 1
      |task:
      |  attributes:
      |    # Columns
      |    # The key
      |    - name: id
      |
      |    # Amount in cents
      |    - name: amount
      |      type: long
      |  domain: d
      |""".stripMargin
  }

  it should "keep the comments above an attribute that changes, and those after the list in place" in {
    val content =
      """version: 1
        |task:
        |  attributes:
        |    - name: id
        |    # Amount in cents, see the contract
        |    - name: amount
        |      type: long
        |    # ----- Write strategy -----
        |  writeStrategy:
        |    type: OVERWRITE
        |""".stripMargin
    val List(id, amount) = attributesOf(content)
    TaskYamlAttributes.replace(
      content,
      List(id, amount.copy(`type` = "decimal"), TableAttribute(name = "x")),
      render,
      path
    ) shouldBe
    """version: 1
      |task:
      |  attributes:
      |    - name: id
      |    # Amount in cents, see the contract
      |    - name: "amount"
      |      type: "decimal"
      |    - name: "x"
      |      type: "string"
      |    # ----- Write strategy -----
      |  writeStrategy:
      |    type: OVERWRITE
      |""".stripMargin
  }

  it should "rewrite a changed item whole, block scalar included, even when a line of it starts with #" in {
    val content =
      """version: 1
        |task:
        |  attributes:
        |    - name: a
        |    - name: b
        |      comment: |
        |        First line
        |        # not a comment
        |  domain: d
        |""".stripMargin
    val List(a, b) = attributesOf(content)
    val updated =
      TaskYamlAttributes.replace(content, List(a, b.copy(`type` = "long")), render, path)
    attributesOf(updated) shouldBe List(a, b.copy(`type` = "long"))
    updated.linesIterator.count(_.trim.startsWith("# not a comment")) shouldBe 0
  }

  it should "rewrite every item of a list whose items are not those of the task, as a list in flow style" in {
    val content = "version: 1\ntask:\n  attributes: [{name: id}, {name: b}]\n  domain: d\n"
    // An attribute read without a type has an empty one, which the serializer leaves out
    TaskYamlAttributes.replace(content, attributesOf(content), render, path) shouldBe
    """version: 1
      |task:
      |  attributes:
      |    - name: "id"
      |    - name: "b"
      |  domain: d
      |""".stripMargin
  }

  it should "keep the line endings of the file, in the items kept and written" in {
    val content = "version: 1\r\ntask:\r\n  attributes:\r\n    - name: id\r\n  domain: d\r\n"
    TaskYamlAttributes.replace(
      content,
      attributesOf(content) :+ TableAttribute(name = "x"),
      render,
      path
    ) shouldBe "version: 1\r\ntask:\r\n  attributes:\r\n    - name: id\r\n    - name: \"x\"\r\n      type: \"string\"\r\n  domain: d\r\n"
  }

  it should "replace an empty list in flow style, and keep the line endings of the file" in {
    val content = "version: 1\r\ntask:\r\n  attributes: []\r\n  domain: d\r\n"
    TaskYamlAttributes.replace(
      content,
      List(TableAttribute(name = "id")),
      render,
      path
    ) shouldBe "version: 1\r\ntask:\r\n  attributes:\r\n    - name: \"id\"\r\n      type: \"string\"\r\n  domain: d\r\n"
  }

  it should "refuse to rewrite attributes written with Jinja or variables, which the sync would render" in {
    List(
      """    - name: id
        |      comment: "{{ reader_sa }}"""".stripMargin,
      """    - name: id
        |      comment: "${reader_sa}"""".stripMargin,
      """    - name: id
        |{% if true %}
        |    - name: secret
        |{% endif %}""".stripMargin,
      // Jinja after the items, which may write more of them
      """    - name: id
        |  {% if true %}{% include "attributes.sl.yml" %}{% endif %}""".stripMargin
    ).foreach { items =>
      val content = s"version: 1\ntask:\n  attributes:\n$items\n  domain: d\n"
      val error = intercept[IllegalStateException] {
        TaskYamlAttributes.replace(content, List(TableAttribute(name = "id")), render, path)
      }
      error.getMessage should include("its attributes use Jinja or variables")
      error.getMessage should include(path)
    }
  }

  it should "refuse to write a new attribute with Jinja or variables, which the loading would render" in {
    val content = "version: 1\ntask:\n  attributes:\n    - name: id\n"
    intercept[IllegalStateException] {
      TaskYamlAttributes.replace(
        content,
        attributesOf(content) :+ TableAttribute(
          name = "amount",
          comment = Some("In {{ currency }}")
        ),
        render,
        path
      )
    }.getMessage should include("attribute amount has a value with Jinja or variables")
  }

  it should "refuse a task key with a value on its line" in {
    intercept[IllegalStateException] {
      TaskYamlAttributes.replace(
        "version: 1\ntask: &task\n  attributes: []\n",
        List(TableAttribute(name = "id")),
        render,
        path
      )
    }.getMessage should include("the task key has a value on its line")
  }

  it should "refuse attributes declared more than once" in {
    val content =
      """version: 1
        |task:
        |{% if true %}
        |  attributes:
        |    - name: id
        |{% else %}
        |  attributes:
        |    - name: other
        |{% endif %}
        |""".stripMargin
    intercept[IllegalStateException] {
      TaskYamlAttributes.replace(content, List(TableAttribute(name = "id")), identity, path)
    }.getMessage should include("more than once")
  }

  it should "refuse a file whose attributes do not read back as written" in {
    // The rendering trims the file: a block scalar that ends it loses its last line break, which
    // it gets back once a new item follows it
    val content =
      """version: 1
        |task:
        |  attributes:
        |    - name: id
        |      comment: |
        |        The key
        |""".stripMargin
    intercept[IllegalStateException] {
      TaskYamlAttributes.replace(
        content,
        attributesOf(content) :+ TableAttribute(name = "amount"),
        render,
        path
      )
    }.getMessage should include("does not read back")
  }

  it should "refuse a file that does not read back as the same task with the new attributes" in {
    // A rendering that changes between the two readings of the file
    var renderings = 0
    val content = "version: 1\ntask:\n  comment: \"{{ n }}\"\n"
    intercept[IllegalStateException] {
      TaskYamlAttributes.replace(
        content,
        List(TableAttribute(name = "id")),
        text => { renderings += 1; text.replace("{{ n }}", renderings.toString) },
        path
      )
    }.getMessage should include("does not read back")
  }

  "create" should "write a task file that declares only the attributes" in {
    val attributes = List(
      TableAttribute(name = "id", `type` = "integer", comment = Some("The key")),
      TableAttribute(name = "name")
    )
    val content = TaskYamlAttributes.create(attributes)
    content shouldBe
    """version: 1
      |task:
      |  attributes:
      |    - name: "id"
      |      type: "integer"
      |      comment: "The key"
      |    - name: "name"
      |      type: "string"
      |""".stripMargin
    attributesOf(content) shouldBe attributes
  }
}
