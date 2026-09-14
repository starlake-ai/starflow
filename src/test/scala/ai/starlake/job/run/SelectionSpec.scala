package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SelectionSpec extends AnyFlatSpec with Matchers {

  /** ext.src (boundary) -> sales.orders (load) -> sales.clean -> sales.revenue and an unrelated
    * ops.audit
    */
  private val dag: RunDag = RunDag(
    nodes = Map(
      "ext.src"       -> RunNode("ext.src", "ext.src", RunNodeType.Boundary),
      "sales.orders"  -> RunNode("sales.orders", "sales.orders", RunNodeType.LoadTable),
      "sales.clean"   -> RunNode("sales.clean", "sales.clean", RunNodeType.Task),
      "sales.revenue" -> RunNode("sales.revenue", "sales.revenue", RunNodeType.Task),
      "ops.audit"     -> RunNode("ops.audit", "ops.audit", RunNodeType.Task)
    ),
    parents = Map(
      "ext.src"       -> Set.empty[String],
      "sales.orders"  -> Set("ext.src"),
      "sales.clean"   -> Set("sales.orders"),
      "sales.revenue" -> Set("sales.clean"),
      "ops.audit"     -> Set.empty[String]
    )
  )

  private val nodeTags: Map[String, Set[String]] = Map(
    "sales.clean"   -> Set("daily"),
    "sales.revenue" -> Set("daily", "WIP"),
    "ops.audit"     -> Set("hourly")
  )

  private def resolved(selects: Seq[String], excludes: Seq[String] = Nil): Selection =
    Selection.resolve(dag, nodeTags, selects, excludes) match {
      case Right(selection) => selection
      case Left(message)    => fail(s"expected a selection, got: $message")
    }

  "Selection" should "select every node when no selector is given" in {
    resolved(Nil).ids shouldBe dag.nodes.keySet
  }

  it should "report no selector matches when nothing was selected explicitly" in {
    val selection = resolved(Nil)
    selection.selectMatches shouldBe empty
    selection.excludeMatches shouldBe empty
  }

  it should "select exactly one node for an exact selector" in {
    resolved(Seq("sales.clean")).ids shouldBe Set("sales.clean")
  }

  it should "not pull in upstreams implicitly" in {
    // Spec section 4: unselected upstreams are not run. If their output is missing the task fails
    // normally.
    resolved(Seq("sales.revenue")).ids should not contain "sales.clean"
  }

  it should "add transitive upstreams for a leading plus" in {
    resolved(Seq("+sales.revenue")).ids shouldBe
    Set("sales.revenue", "sales.clean", "sales.orders", "ext.src")
  }

  it should "add transitive downstreams for a trailing plus" in {
    resolved(Seq("sales.clean+")).ids shouldBe Set("sales.clean", "sales.revenue")
  }

  it should "add both directions for a surrounding plus" in {
    resolved(Seq("+sales.clean+")).ids shouldBe
    Set("ext.src", "sales.orders", "sales.clean", "sales.revenue")
  }

  it should "select a whole domain for a wildcard" in {
    resolved(Seq("sales.*")).ids shouldBe Set("sales.orders", "sales.clean", "sales.revenue")
  }

  it should "select by tag, case-insensitively on both sides" in {
    resolved(Seq("tag:daily")).ids shouldBe Set("sales.clean", "sales.revenue")
    resolved(Seq("tag:wip")).ids shouldBe Set("sales.revenue")
  }

  it should "union multiple selectors" in {
    resolved(Seq("ops.audit", "sales.clean")).ids shouldBe Set("ops.audit", "sales.clean")
  }

  it should "let exclusion win over selection" in {
    resolved(Seq("sales.*"), excludes = Seq("sales.revenue")).ids shouldBe
    Set("sales.orders", "sales.clean")
  }

  it should "expand operators inside an exclusion" in {
    resolved(Seq("sales.*"), excludes = Seq("+sales.clean")).ids shouldBe Set("sales.revenue")
  }

  it should "exclude from the whole project when no select is given" in {
    resolved(Nil, excludes = Seq("tag:daily")).ids shouldBe
    Set("ext.src", "sales.orders", "ops.audit")
  }

  it should "match a node on its last two segments" in {
    // Lineage can name a table with a project or catalog prefix; a two-part reference must still
    // resolve, the same way DagBuilder classifies load tables.
    val prefixed = RunDag(
      nodes = Map(
        "proj.sales.revenue" ->
        RunNode("proj.sales.revenue", "proj.sales.revenue", RunNodeType.Task)
      ),
      parents = Map("proj.sales.revenue" -> Set.empty[String])
    )
    Selection.resolve(prefixed, Map.empty, Seq("sales.revenue"), Nil).map(_.ids) shouldBe
    Right(Set("proj.sales.revenue"))
    Selection.resolve(prefixed, Map.empty, Seq("sales.*"), Nil).map(_.ids) shouldBe
    Right(Set("proj.sales.revenue"))
  }

  it should "count only executable nodes as matches" in {
    // ext.src is a boundary: it is selected for ordering but it is not a task, so a selector that
    // reaches it must not claim to have matched it.
    val selection = resolved(Seq("+sales.revenue"))
    selection.ids should contain("ext.src")
    selection.selectMatches shouldBe List(SelectorMatch("+sales.revenue", 3))
  }

  it should "report zero matches for a selector that matched nothing" in {
    val selection = resolved(Seq("sales.clean", "nope.nothing"))
    selection.ids shouldBe Set("sales.clean")
    selection.selectMatches shouldBe
    List(SelectorMatch("sales.clean", 1), SelectorMatch("nope.nothing", 0))
  }

  it should "count an exclusion by what it actually removed" in {
    // tag:daily expands to two nodes, but only one of them was selected.
    val selection = resolved(Seq("sales.clean"), excludes = Seq("tag:daily"))
    selection.ids shouldBe empty
    selection.excludeMatches shouldBe List(SelectorMatch("tag:daily", 1))
  }

  it should "reject a malformed select expression" in {
    Selection.resolve(dag, nodeTags, Seq("a.b.c"), Nil).isLeft shouldBe true
  }

  it should "reject a malformed exclude expression" in {
    Selection.resolve(dag, nodeTags, Nil, Seq("*")).isLeft shouldBe true
  }

  it should "report the first malformed expression without evaluating the graph" in {
    Selection.resolve(dag, nodeTags, Seq("sales.clean", "a.b.c"), Nil) match {
      case Left(message) => message should include("a.b.c")
      case Right(value)  => fail(s"expected a rejection, got: $value")
    }
  }
}
