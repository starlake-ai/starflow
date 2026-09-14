package ai.starlake.job.run

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class FileRunStoreSpec extends RunStoreContractSpec {

  private def withTempRoot(test: Path => Unit): Unit = {
    val root = Files.createTempDirectory("run-store-spec")
    try test(root)
    finally {
      Files.walk(root).iterator().asScala.toList.reverse.foreach(Files.deleteIfExists)
    }
  }

  private var currentRoot: Path = _

  override def withStore(test: RunStore => Unit): Unit = withTempRoot { root =>
    currentRoot = root
    val store = new FileRunStore(root)
    try test(store)
    finally store.close()
  }

  /** What a new process sees: another store over the same directory. */
  override def reopenStore(previous: RunStore): RunStore = new FileRunStore(currentRoot)

  private def linesOf(file: Path): List[String] =
    Files.readAllLines(file, StandardCharsets.UTF_8).asScala.toList

  "FileRunStore" should "name attempt 1 events.jsonl and later attempts events.N.jsonl" in
    withTempRoot { root =>
      val store = new FileRunStore(root)
      try {
        store.start(header("r1"))
        store.reopen("r1")
        store.append(header("r1").copy(attempt = 2))
      } finally store.close()

      Files.exists(root.resolve("r1").resolve("events.jsonl")) shouldBe true
      Files.exists(root.resolve("r1").resolve("events.2.jsonl")) shouldBe true
    }

  it should "order attempts numerically, not lexically" in withTempRoot { root =>
    val store = new FileRunStore(root)
    try {
      store.start(header("r1"))
      store.append(event("r1", 1, 2, RunLogEventType.TaskSucceeded, Some("t1")))
      // Ten attempts: with lexical ordering "events.10.jsonl" would sort before "events.2.jsonl"
      // and the attempt count would come out wrong.
      (2 to 10).foreach { attempt =>
        store.reopen("r1") shouldBe attempt
        store.append(header("r1").copy(attempt = attempt))
        store.append(event("r1", attempt, 2, RunLogEventType.TaskSucceeded, Some(s"t$attempt")))
      }
    } finally store.close()

    val history = new FileRunStore(root).read("r1").getOrElse(fail("expected a history"))
    history.attempts shouldBe 10
    history.succeeded should contain("t10")
  }

  it should "drop a final line torn by a kill" in withTempRoot { root =>
    val store = new FileRunStore(root)
    try {
      store.start(header("r1"))
      store.append(event("r1", 1, 2, RunLogEventType.TaskSucceeded, Some("a")))
    } finally store.close()

    val file = root.resolve("r1").resolve("events.jsonl")
    // Simulate the kill: a half-written last line, exactly what a flush interrupted mid-write
    // leaves behind.
    Files.write(file, (linesOf(file).mkString("\n") + "\n{\"runId\":\"r1\",\"att").getBytes(StandardCharsets.UTF_8))

    val history = new FileRunStore(root).read("r1").getOrElse(fail("expected a history"))
    history.succeeded shouldBe Set("a")
  }

  it should "refuse to read corruption that is not at the end of a file" in withTempRoot { root =>
    val store = new FileRunStore(root)
    try {
      store.start(header("r1"))
      store.append(event("r1", 1, 2, RunLogEventType.TaskSucceeded, Some("a")))
      store.append(event("r1", 1, 3, RunLogEventType.TaskSucceeded, Some("b")))
    } finally store.close()

    val file = root.resolve("r1").resolve("events.jsonl")
    val damaged = linesOf(file).updated(1, "{ not json")
    Files.write(file, damaged.mkString("\n").getBytes(StandardCharsets.UTF_8))

    // We tore nothing here: a bad line in the middle is damage we did not cause, and skipping it
    // would silently drop a success and re-execute a task that already ran.
    a[RunStoreException] should be thrownBy new FileRunStore(root).read("r1")
  }

  it should "survive an abrupt close, leaving every flushed event readable" in withTempRoot { root =>
    val store = new FileRunStore(root)
    store.start(header("r1"))
    store.append(event("r1", 1, 2, RunLogEventType.TaskSucceeded, Some("a")))
    // no close(): every append flushed, so the bytes are with the OS already
    new FileRunStore(root).read("r1").map(_.succeeded) shouldBe Some(Set("a"))
  }

  it should "ignore a directory that holds no run log" in withTempRoot { root =>
    Files.createDirectories(root.resolve("not-a-run"))
    val store = new FileRunStore(root)
    try {
      store.read("not-a-run") shouldBe None
      store.latest() shouldBe None
    } finally store.close()
  }
}
