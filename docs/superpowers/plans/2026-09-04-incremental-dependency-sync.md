# Incremental Dependency Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `starlake upgrade` and repeated `starlake install` download only what is missing or out of date, instead of deleting and re-fetching every dependency on every run.

**Architecture:** A pure reconciler in `ai.starlake.setup` diffs a desired-state list against the contents of `bin/deps`, `bin/sl` and `bin/deps/python-libs`, producing a `SyncPlan` (download / delete / up-to-date). `Setup.java` builds the plain-data inputs, probes remote sizes over HTTP, prints the merged plan, then applies it. Everything lands in the installer, so Unix, Windows and Docker are fixed by the same change.

**Tech Stack:** Java 17 (`--release 17`, no Scala library available at runtime inside `setup.jar`), `java.net.http.HttpClient`, sbt 1.11.5, ScalaTest 3.x for the reconciler spec, bash + cmd for the launcher flags.

**Spec:** `docs/superpowers/specs/2026-09-04-incremental-dependency-sync-design.md`

## Global Constraints

- `setup.jar` runs as `java -cp setup.jar Setup` with **no Scala library on the classpath**. Every new production class must be plain Java under `src/main/java/`. Never reference Scala or any third-party library from these classes.
- Java sources compile with `--release 17` and `-Xlint` (`build.sbt:9-13`). Warnings are visible; do not introduce raw types or unchecked casts.
- `Setup.java` is in the **unnamed package**. It may `import ai.starlake.setup.*` (legal), but nothing in a named package may reference `Setup` (illegal). All tests therefore target `ai.starlake.setup`, never `Setup`.
- `distrib/setup.jar` is a **committed binary**, rebuilt by `sbt packageSetup` and pushed by `scripts/local-release.sh:428`. Any change to the classes it contains requires regenerating it (Task 12).
- Scala test sources are formatted by scalafmt, which runs automatically on compile. Java sources are not formatted by scalafmt; match the surrounding style in `Setup.java` (4-space indent, braces on the same line).
- Tests run sequentially and forked (`Test / parallelExecution := false`, `Test / fork := true`). The reconciler spec must not start Spark and must not extend `TestHelper`; it extends `AnyFlatSpec with Matchers` directly, like `src/test/scala/ai/starlake/config/ConnectionInfoFlightSqlSpec.scala`.
- No new third-party dependency may be added for this work.
- Existing behavior that must not regress: `starlake reinstall` keeps `rm -rf bin/spark bin/deps bin/sl`; `bin/spark`, `bin/api` and the Windows `bin/hadoop` provisioning are untouched.

---

## File Structure

**Created:**

| File | Responsibility |
|---|---|
| `src/main/java/ai/starlake/setup/Artifact.java` | Plain-data description of one desired artifact: label, file name, url, ownership prefixes, enabled flag. The bridge type between `Setup.java` and the reconciler. |
| `src/main/java/ai/starlake/setup/SyncPlan.java` | The diff result: `toDownload`, `toDelete`, `upToDate`, plus merge and human-readable rendering. |
| `src/main/java/ai/starlake/setup/DependencySync.java` | The pure reconciler: ownership-prefix derivation and `reconcile(...)`. No static state, no network, no file writes. |
| `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala` | ScalaTest spec for prefix derivation, the prefix invariants, and every reconciliation case. |

**Modified:**

| File | Change |
|---|---|
| `build.sbt:312-337` | `packageSetup` scans the classes directory instead of naming three class files, and preserves relative paths so packaged classes keep their package directories. |
| `src/main/java/Setup.java` | Category list, size probing, plan printing, apply, and the three wired roots. |
| `distrib/starlake.sh` | `--dry-run` / `--force` flags on `install` and `upgrade`, exported to the Setup subprocess. |
| `distrib/starlake.cmd` | Same two flags. |
| `distrib/setup.jar` | Regenerated. |

---

## Task 1: Package every Setup class into setup.jar

`packageSetup` names exactly three class files. Adding any nested class silently omits it and the installer dies at runtime with `NoClassDefFoundError`. Nothing else in this plan is safe until this is fixed, so it goes first.

**Files:**
- Modify: `build.sbt:312-337`

**Interfaces:**
- Consumes: nothing.
- Produces: a `distrib/setup.jar` that contains `Setup.class`, every `Setup$*.class`, and every class under `ai/starlake/setup/` with its package directory preserved.

- [ ] **Step 1: Record the current jar contents as the baseline**

```bash
unzip -l distrib/setup.jar
```

Expected: `META-INF/MANIFEST.MF`, `Setup.class`, `Setup$UserPwdAuth.class`, `Setup$ResourceDependency.class`. Write these four names down; Step 5 checks they are all still present.

- [ ] **Step 2: Replace the hardcoded class list with a directory scan**

In `build.sbt`, replace the body of `packageSetup` (the `val scalaMajorVersion = ...` line through the closing `)` of the `zipFile(...)` call) with:

```scala
  val scalaMajorVersion = scalaVersion.value.split('.').take(2).mkString(".")
  val classesDir = Paths.get(s"target/scala-$scalaMajorVersion/classes")
  // Scan instead of naming class files one by one: nested classes (Setup$Managed,
  // SyncPlan$Download, ...) are created by the compiler as the installer grows, and a
  // hardcoded list silently omits them - setup.jar then fails at run time with
  // NoClassDefFoundError. Paths are kept RELATIVE to classesDir so classes in a real
  // package land under their package directory inside the jar.
  val setupClasses: List[java.nio.file.Path] = {
    val root = classesDir.resolve("Setup.class")
    val nested = IO.listFiles(classesDir.toFile)
      .filter(f => f.getName.startsWith("Setup$") && f.getName.endsWith(".class"))
      .map(_.toPath)
      .toList
    val packaged = {
      val pkgDir = classesDir.resolve("ai/starlake/setup").toFile
      if (pkgDir.isDirectory)
        IO.listFiles(pkgDir).filter(_.getName.endsWith(".class")).map(_.toPath).toList
      else Nil
    }
    (root :: nested ::: packaged).sorted
  }
  if (!setupClasses.exists(_.endsWith("Setup.class")))
    sys.error(s"Setup.class not found under $classesDir - run `sbt compile` first")
  val to = Paths.get("distrib/setup.jar")
  zipFile(setupClasses, to, classesDir)
```

- [ ] **Step 3: Make `zipFile` preserve relative paths**

Still in `build.sbt`, change the `zipFile` helper's signature and its `IO.jar` call. Replace:

```scala
  def zipFile(from: List[java.nio.file.Path], to: java.nio.file.Path): Unit = {
```

with:

```scala
  def zipFile(from: List[java.nio.file.Path], to: java.nio.file.Path, base: java.nio.file.Path): Unit = {
```

and replace:

```scala
    IO.jar(from.map(f => f.toFile -> f.toFile.getName()), to.toFile, manifest)
```

with:

```scala
    // relativize against the classes dir: `getName` alone flattens ai/starlake/setup/X.class
    // to X.class, which the JVM then refuses to load as ai.starlake.setup.X
    IO.jar(from.map(f => f.toFile -> base.relativize(f).toString.replace('\\', '/')), to.toFile, manifest)
```

- [ ] **Step 4: Rebuild the jar**

Run: `sbt packageSetup`
Expected: `[success]`, no error about `Setup.class not found`.

- [ ] **Step 5: Verify the jar still contains the baseline classes and still runs**

```bash
unzip -l distrib/setup.jar
java -cp distrib/setup.jar Setup 2>&1 | head -2
```

Expected: the same four entries from Step 1, and the `java` invocation prints `Please specify the target directory` (its no-args path). If it prints a `NoClassDefFoundError` or `ClassNotFoundException`, the scan or the relativization is wrong.

- [ ] **Step 6: Commit**

```bash
git add build.sbt distrib/setup.jar
git commit -m "build: package all Setup classes into setup.jar by scanning, not by name"
```

---

## Task 2: The Artifact bridge type

**Files:**
- Create: `src/main/java/ai/starlake/setup/Artifact.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `ai.starlake.setup.Artifact` with public final fields `label` (String), `fileName` (String), `url` (String), `ownershipPrefixes` (`List<String>`), `enabled` (boolean), and constructor `Artifact(String label, String fileName, String url, List<String> ownershipPrefixes, boolean enabled)`. Used by every later task.

- [ ] **Step 1: Write the class**

```java
package ai.starlake.setup;

import java.util.Collections;
import java.util.List;

/**
 * One artifact the installer manages, described as plain data so the reconciler never
 * needs to know about Setup's ResourceDependency.
 *
 * <p>A disabled artifact still carries its ownership prefixes: that is what lets the
 * reconciler remove the jars of a category the user has just turned off.
 */
public final class Artifact {

    /** Category label used in the printed plan, e.g. "Snowflake". */
    public final String label;

    /** File name this artifact must have on disk once installed. */
    public final String fileName;

    /** URL the artifact is downloaded from. */
    public final String url;

    /**
     * Name fragments that identify a file on disk as belonging to this artifact,
     * whatever its version. Matched with {@link String#contains(CharSequence)} against
     * the file NAME - never the path.
     */
    public final List<String> ownershipPrefixes;

    /** Whether this artifact is wanted for the current run. */
    public final boolean enabled;

    public Artifact(String label, String fileName, String url, List<String> ownershipPrefixes, boolean enabled) {
        this.label = label;
        this.fileName = fileName;
        this.url = url;
        this.ownershipPrefixes = Collections.unmodifiableList(ownershipPrefixes);
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return fileName + (enabled ? "" : " (disabled)");
    }
}
```

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: `[success]`, no `-Xlint` warnings mentioning `Artifact`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/ai/starlake/setup/Artifact.java
git commit -m "feat(setup): add Artifact, the plain-data bridge type for dependency sync"
```

---

## Task 3: Ownership prefix derivation

The rule that decides which files on disk belong to which artifact. Getting this wrong either deletes the wrong jar or leaks stale ones, so it is built and tested on its own before anything uses it.

**Files:**
- Create: `src/main/java/ai/starlake/setup/DependencySync.java`
- Create: `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `public static String derivePrefix(String url, String fileName)` on `ai.starlake.setup.DependencySync`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`:

```scala
package ai.starlake.setup

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DependencySyncSpec extends AnyFlatSpec with Matchers {

  private def prefix(url: String): String =
    DependencySync.derivePrefix(url, url.substring(url.lastIndexOf('/') + 1))

  "derivePrefix" should "strip the version taken from the second-to-last url segment" in {
    prefix(
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar"
    ) shouldBe "postgresql-"
  }

  it should "handle an artifact whose file name differs from its label" in {
    prefix(
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar"
    ) shouldBe "bundle-"
  }

  it should "strip a leading v from a github release tag segment" in {
    prefix(
      "https://github.com/starlake-ai/spark-redshift/releases/download/v7.0.0/spark-redshift_2.13-7.0.0.jar"
    ) shouldBe "spark-redshift_2.13-"
  }

  it should "not over-truncate a file name that embeds a version-looking token" in {
    prefix(
      "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-4.1-bigquery/0.44.2-preview/spark-4.1-bigquery-0.44.2-preview.jar"
    ) shouldBe "spark-4.1-bigquery-"
  }

  it should "fall back to the first digit-led segment when the url carries no version segment" in {
    prefix(
      "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_airflow-0.6.11-py3-none-any.whl"
    ) shouldBe "starlake_airflow-"
  }

  it should "fall back to the whole name when there is no version anywhere" in {
    prefix(
      "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
    ) shouldBe "winutils.exe"
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: compilation failure, `not found: value DependencySync` (or `object DependencySync is not a member of package ai.starlake.setup`).

- [ ] **Step 3: Implement `derivePrefix`**

Create `src/main/java/ai/starlake/setup/DependencySync.java`:

```java
package ai.starlake.setup;

/**
 * Pure reconciliation between the artifacts the installer wants and the files already
 * on disk. No static state, no network, no writes: everything it needs is passed in.
 */
public final class DependencySync {

    private DependencySync() {
    }

    /**
     * Name fragment shared by every version of an artifact, used to spot superseded
     * copies on disk.
     *
     * <p>Maven and GitHub Releases URLs both put the version in the second-to-last path
     * segment ({@code .../bundle/2.29.52/bundle-2.29.52.jar}, {@code .../download/v7.0.0/...}),
     * and the file name always contains it, so truncating the name at that version is
     * exact - unlike guessing at the first digit, which turns
     * {@code spark-4.1-bigquery-0.44.2-preview.jar} into a dangerously broad {@code spark-}.
     *
     * <p>When the URL carries no version segment (the python wheels are served from a flat
     * directory), fall back to the name up to the first {@code -} followed by a digit, and
     * finally to the whole name.
     */
    public static String derivePrefix(String url, String fileName) {
        String versionSegment = versionSegment(url);
        if (versionSegment != null && !versionSegment.isEmpty()) {
            int at = fileName.indexOf(versionSegment);
            if (at > 0) {
                return fileName.substring(0, at);
            }
        }
        for (int i = 1; i < fileName.length() - 1; i++) {
            if (fileName.charAt(i) == '-' && Character.isDigit(fileName.charAt(i + 1))) {
                return fileName.substring(0, i + 1);
            }
        }
        return fileName;
    }

    /** Second-to-last path segment of the url, with any leading {@code v} stripped. */
    private static String versionSegment(String url) {
        String path = url;
        int query = path.indexOf('?');
        if (query >= 0) {
            path = path.substring(0, query);
        }
        int last = path.lastIndexOf('/');
        if (last <= 0) {
            return null;
        }
        int previous = path.lastIndexOf('/', last - 1);
        if (previous < 0) {
            return null;
        }
        String segment = path.substring(previous + 1, last);
        if (segment.length() > 1 && segment.charAt(0) == 'v' && Character.isDigit(segment.charAt(1))) {
            segment = segment.substring(1);
        }
        return segment;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: `Tests: succeeded 6, failed 0`.

- [ ] **Step 5: Add the golden prefix table and the collision invariant**

This is the safety net that keeps deletions correct as dependencies are added.
Append inside the class in `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`:

```scala
  // Every distinct URL shape the installer downloads from, with the prefix each one must
  // yield. Versions here are illustrative and deliberately NOT tied to the real pinned
  // values: what is asserted is the artifact-id SHAPE, which is what ownership depends on,
  // so a routine version bump must never break this table. Add a row when you add a
  // dependency with a new naming shape.
  private val goldenPrefixes: List[(String, String)] = List(
    "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-4.1-bigquery/9.9.9-preview/spark-4.1-bigquery-9.9.9-preview.jar" -> "spark-4.1-bigquery-",
    "https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/9.9.9/delta-spark_4.1_2.13-9.9.9.jar"                           -> "delta-spark_4.1_2.13-",
    "https://repo1.maven.org/maven2/io/delta/delta-storage/9.9.9/delta-storage-9.9.9.jar"                                         -> "delta-storage-",
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/9.9.9/iceberg-spark-runtime-4.1_2.13-9.9.9.jar" -> "iceberg-spark-runtime-4.1_2.13-",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/9.9.9/hadoop-azure-9.9.9.jar"                                  -> "hadoop-azure-",
    "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/9.9.9/azure-storage-9.9.9.jar"                              -> "azure-storage-",
    "https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/9.9.9.v99999999/jetty-server-9.9.9.v99999999.jar"              -> "jetty-server-",
    "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/9.9.9/snowflake-jdbc-9.9.9.jar"                                  -> "snowflake-jdbc-",
    "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.13/9.9.9-spark_4.1/spark-snowflake_2.13-9.9.9-spark_4.1.jar"  -> "spark-snowflake_2.13-",
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/9.9.9/postgresql-9.9.9.jar"                                         -> "postgresql-",
    "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/9.9.9.9/duckdb_jdbc-9.9.9.9.jar"                                       -> "duckdb_jdbc-",
    "https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/9.9.9/flight-sql-jdbc-driver-9.9.9.jar"               -> "flight-sql-jdbc-driver-",
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/9.9.9/bundle-9.9.9.jar"                                         -> "bundle-",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/9.9.9/hadoop-aws-9.9.9.jar"                                      -> "hadoop-aws-",
    "https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/9.9.9/redshift-jdbc42-9.9.9.jar"                          -> "redshift-jdbc42-",
    "https://github.com/starlake-ai/spark-redshift/releases/download/v9.9.9/spark-redshift_2.13-9.9.9.jar"                        -> "spark-redshift_2.13-",
    "https://github.com/starlake-ai/starflow/releases/download/v9.9.9/starlake-core_2.13-9.9.9-assembly.jar"                      -> "starlake-core_2.13-",
    "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/9.9.9/kafka-schema-registry-client-9.9.9.jar"  -> "kafka-schema-registry-client-",
    "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/9.9.9/kafka-avro-serializer-9.9.9.jar"                -> "kafka-avro-serializer-",
    "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/9.9.9/mariadb-java-client-9.9.9.jar"                     -> "mariadb-java-client-",
    "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/9.9.9/clickhouse-jdbc-9.9.9-all.jar"                           -> "clickhouse-jdbc-",
    "https://repo1.maven.org/maven2/io/trino/trino-jdbc/999/trino-jdbc-999.jar"                                                   -> "trino-jdbc-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_airflow-9.9.9-py3-none-any.whl"   -> "starlake_airflow-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_dagster-9.9.9-py3-none-any.whl"   -> "starlake_dagster-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_orchestration-9.9.9-py3-none-any.whl" -> "starlake_orchestration-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_snowflake-9.9.9-py3-none-any.whl" -> "starlake_snowflake-"
  )

  it should "derive the documented prefix for every dependency naming shape" in {
    goldenPrefixes.foreach { case (url, expected) =>
      withClue(s"url=$url: ") { prefix(url) shouldBe expected }
    }
  }

```

The matching collision invariant needs `ownerOf`, which Task 5 introduces; it is
added there.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: `Tests: succeeded 7, failed 0`. A failure names the offending URL via
the `withClue`.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/ai/starlake/setup/DependencySync.java src/test/scala/ai/starlake/setup/DependencySyncSpec.scala
git commit -m "feat(setup): derive an ownership prefix from each dependency url"
```

---

## Task 4: The SyncPlan result type

**Files:**
- Create: `src/main/java/ai/starlake/setup/SyncPlan.java`
- Modify: `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`

**Interfaces:**
- Consumes: `Artifact` (Task 2).
- Produces: `ai.starlake.setup.SyncPlan` with public methods `add(Download)`, `add(Deletion)`, `addUpToDate(File)`, `getToDownload(): List<Download>`, `getToDelete(): List<Deletion>`, `getUpToDate(): List<File>`, `isEmpty(): boolean`, `bytesToDownload(): long`, `mergeFrom(SyncPlan): void`, `render(String header): String`. Nested `SyncPlan.Download` (fields `artifact`, `target`, `size`) and `SyncPlan.Deletion` (fields `file`, `reason`).

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`, inside the class:

```scala
  "SyncPlan.render" should "collapse to one line when there is nothing to do" in {
    val plan = new SyncPlan()
    plan.addUpToDate(new java.io.File("/tmp/a.jar"))
    plan.addUpToDate(new java.io.File("/tmp/b.jar"))
    plan.isEmpty shouldBe true
    plan.render("Dependency plan") shouldBe
    "All 2 dependencies up to date, nothing to download."
  }

  it should "list only what changes, with sizes and reasons" in {
    val plan     = new SyncPlan()
    val artifact = new Artifact(
      "Postgres",
      "postgresql-42.7.11.jar",
      "https://example.invalid/postgresql-42.7.11.jar",
      java.util.List.of("postgresql-"),
      true
    )
    plan.addUpToDate(new java.io.File("/tmp/kept.jar"))
    plan.add(new SyncPlan.Download(artifact, new java.io.File("/tmp/postgresql-42.7.11.jar"), 1048576L))
    plan.add(new SyncPlan.Deletion(new java.io.File("/tmp/postgresql-42.7.10.jar"), "superseded"))
    plan.isEmpty shouldBe false
    plan.bytesToDownload shouldBe 1048576L

    val rendered = plan.render("Dependency plan")
    rendered should include("Dependency plan")
    rendered should include("= 1 up to date")
    rendered should include("+ 1 to download (1 MB)")
    rendered should include("postgresql-42.7.11.jar")
    rendered should include("- 1 to remove")
    rendered should include("postgresql-42.7.10.jar")
    rendered should include("(superseded)")
    rendered should not include "kept.jar"
  }

  it should "report an unknown download size rather than a bogus zero" in {
    val plan     = new SyncPlan()
    val artifact = new Artifact("Core", "core.jar", "https://example.invalid/core.jar", java.util.List.of("core"), true)
    plan.add(new SyncPlan.Download(artifact, new java.io.File("/tmp/core.jar"), -1L))
    plan.bytesToDownload shouldBe 0L
    plan.render("Dependency plan") should include("(unknown size)")
  }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: compilation failure, `not found: type SyncPlan`.

- [ ] **Step 3: Implement `SyncPlan`**

Create `src/main/java/ai/starlake/setup/SyncPlan.java`:

```java
package ai.starlake.setup;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The difference between the artifacts the installer wants and what is on disk. */
public final class SyncPlan {

    /** An artifact that must be fetched, with the remote size when it could be determined. */
    public static final class Download {
        public final Artifact artifact;
        public final File target;
        /** Remote byte size, or -1 when it could not be determined. */
        public final long size;

        public Download(Artifact artifact, File target, long size) {
            this.artifact = artifact;
            this.target = target;
            this.size = size;
        }
    }

    /** A file that must go, with the reason shown to the user. */
    public static final class Deletion {
        public final File file;
        public final String reason;

        public Deletion(File file, String reason) {
            this.file = file;
            this.reason = reason;
        }
    }

    private final List<Download> toDownload = new ArrayList<>();
    private final List<Deletion> toDelete = new ArrayList<>();
    private final List<File> upToDate = new ArrayList<>();

    public void add(Download download) {
        toDownload.add(download);
    }

    public void add(Deletion deletion) {
        toDelete.add(deletion);
    }

    public void addUpToDate(File file) {
        upToDate.add(file);
    }

    public List<Download> getToDownload() {
        return Collections.unmodifiableList(toDownload);
    }

    public List<Deletion> getToDelete() {
        return Collections.unmodifiableList(toDelete);
    }

    public List<File> getUpToDate() {
        return Collections.unmodifiableList(upToDate);
    }

    public boolean isEmpty() {
        return toDownload.isEmpty() && toDelete.isEmpty();
    }

    /** Total of the KNOWN download sizes; artifacts of unknown size contribute nothing. */
    public long bytesToDownload() {
        long total = 0;
        for (Download download : toDownload) {
            if (download.size > 0) {
                total += download.size;
            }
        }
        return total;
    }

    public void mergeFrom(SyncPlan other) {
        toDownload.addAll(other.toDownload);
        toDelete.addAll(other.toDelete);
        upToDate.addAll(other.upToDate);
    }

    /**
     * Human-readable plan. The no-op path - by far the most common one once this feature
     * lands - is deliberately a single line: a fast upgrade should look fast, not silent.
     */
    public String render(String header) {
        if (isEmpty()) {
            return "All " + upToDate.size() + " dependencies up to date, nothing to download.";
        }
        StringBuilder sb = new StringBuilder(header).append("\n");
        sb.append("  = ").append(upToDate.size()).append(" up to date\n");
        if (!toDownload.isEmpty()) {
            sb.append("  + ").append(toDownload.size()).append(" to download (")
              .append(humanSize(bytesToDownload())).append(")\n");
            for (Download download : toDownload) {
                sb.append("      ").append(download.artifact.fileName).append("  ")
                  .append(download.size > 0 ? "(" + humanSize(download.size) + ")" : "(unknown size)")
                  .append("\n");
            }
        }
        if (!toDelete.isEmpty()) {
            sb.append("  - ").append(toDelete.size()).append(" to remove\n");
            for (Deletion deletion : toDelete) {
                sb.append("      ").append(deletion.file.getName()).append("  (")
                  .append(deletion.reason).append(")\n");
            }
        }
        return sb.toString();
    }

    private static String humanSize(long bytes) {
        if (bytes >= 1024L * 1024L) {
            return (bytes / 1024L / 1024L) + " MB";
        }
        if (bytes >= 1024L) {
            return (bytes / 1024L) + " KB";
        }
        return bytes + " B";
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: `Tests: succeeded 10, failed 0`.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/ai/starlake/setup/SyncPlan.java src/test/scala/ai/starlake/setup/DependencySyncSpec.scala
git commit -m "feat(setup): add SyncPlan, the dependency diff result and its rendering"
```

---

## Task 5: The reconciler

**Files:**
- Modify: `src/main/java/ai/starlake/setup/DependencySync.java`
- Modify: `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`

**Interfaces:**
- Consumes: `Artifact` (Task 2), `SyncPlan` (Task 4), `derivePrefix` (Task 3).
- Produces: `public static SyncPlan reconcile(List<Artifact> artifacts, File dir, Map<String, Long> remoteSizes, boolean force)`. `remoteSizes` is keyed by `Artifact.url`; a missing key or a value `<= 0` means "size unknown". Used by Tasks 8, 9 and 10.

- [ ] **Step 1: Write the failing test**

Append to `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`, inside the class:

```scala
  private val emptySizes = new java.util.HashMap[String, java.lang.Long]()

  private def artifact(
    label: String,
    fileName: String,
    url: String,
    enabled: Boolean = true,
    legacyLabel: String = null
  ): Artifact = {
    val prefixes = new java.util.ArrayList[String]()
    prefixes.add(DependencySync.derivePrefix(url, fileName))
    if (legacyLabel != null) prefixes.add(legacyLabel)
    new Artifact(label, fileName, url, prefixes, enabled)
  }

  private def tempDirWith(names: String*): java.io.File = {
    val dir = java.nio.file.Files.createTempDirectory("sl-sync-").toFile
    dir.deleteOnExit()
    names.foreach { n =>
      val f = new java.io.File(dir, n)
      java.nio.file.Files.write(f.toPath, Array.fill[Byte](10)(0))
      f.deleteOnExit()
    }
    dir
  }

  private def sizes(pairs: (String, Long)*): java.util.Map[String, java.lang.Long] = {
    val m = new java.util.HashMap[String, java.lang.Long]()
    pairs.foreach { case (k, v) => m.put(k, java.lang.Long.valueOf(v)) }
    m
  }

  private val pgUrl  = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar"
  private val pgArt  = artifact("Postgres", "postgresql-42.7.11.jar", pgUrl)
  private val sfUrl  = "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/4.3.3/snowflake-jdbc-4.3.3.jar"

  "reconcile" should "download everything into an empty directory" in {
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), tempDirWith(), emptySizes, false)
    plan.getToDownload should have size 1
    plan.getToDownload.get(0).artifact.fileName shouldBe "postgresql-42.7.11.jar"
    plan.getToDelete shouldBe empty
    plan.getUpToDate shouldBe empty
  }

  it should "keep a present file whose size matches the remote size" in {
    val dir  = tempDirWith("postgresql-42.7.11.jar")
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), dir, sizes(pgUrl -> 10L), false)
    plan.getToDownload shouldBe empty
    plan.getToDelete shouldBe empty
    plan.getUpToDate should have size 1
    plan.isEmpty shouldBe true
  }

  it should "re-download a present file whose size does not match" in {
    val dir  = tempDirWith("postgresql-42.7.11.jar")
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), dir, sizes(pgUrl -> 999999L), false)
    plan.getToDownload should have size 1
    plan.getUpToDate shouldBe empty
  }

  it should "keep a present file when the remote size is unknown, so an offline run is a no-op" in {
    val dir  = tempDirWith("postgresql-42.7.11.jar")
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), dir, sizes(pgUrl -> -1L), false)
    plan.getToDownload shouldBe empty
    plan.getUpToDate should have size 1
  }

  it should "remove a superseded version and fetch the new one" in {
    val dir  = tempDirWith("postgresql-42.7.10.jar")
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), dir, emptySizes, false)
    plan.getToDownload should have size 1
    plan.getToDelete should have size 1
    plan.getToDelete.get(0).file.getName shouldBe "postgresql-42.7.10.jar"
    plan.getToDelete.get(0).reason shouldBe "superseded"
  }

  it should "remove the jars of a disabled category and download nothing for it" in {
    val disabled = artifact("Snowflake", "snowflake-jdbc-4.3.3.jar", sfUrl, enabled = false)
    val dir      = tempDirWith("snowflake-jdbc-4.3.3.jar")
    val plan     = DependencySync.reconcile(java.util.List.of(disabled), dir, emptySizes, false)
    plan.getToDownload shouldBe empty
    plan.getToDelete should have size 1
    plan.getToDelete.get(0).reason shouldBe "Snowflake disabled"
  }

  it should "leave a hand-copied unknown jar alone" in {
    val dir  = tempDirWith("postgresql-42.7.11.jar", "my-private-driver-1.0.jar")
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), dir, sizes(pgUrl -> 10L), false)
    plan.getToDelete shouldBe empty
    plan.getUpToDate.size shouldBe 1
  }

  it should "clean the current bundle jar as well as the legacy aws-java-sdk-bundle jar" in {
    val awsUrl = "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar"
    val aws    = artifact("Redshift", "bundle-2.29.52.jar", awsUrl, legacyLabel = "aws-java-sdk-bundle")
    val dir    = tempDirWith("bundle-2.20.0.jar", "aws-java-sdk-bundle-1.12.500.jar")
    val plan   = DependencySync.reconcile(java.util.List.of(aws), dir, emptySizes, false)
    plan.getToDelete.size shouldBe 2
    plan.getToDownload should have size 1
  }

  it should "not delete anything just because the directory path contains an artefact name" in {
    val root = java.nio.file.Files.createTempDirectory("postgresql-").toFile
    root.deleteOnExit()
    val f = new java.io.File(root, "my-private-driver-1.0.jar")
    java.nio.file.Files.write(f.toPath, Array.fill[Byte](10)(0))
    f.deleteOnExit()
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), root, emptySizes, false)
    plan.getToDelete shouldBe empty
  }

  it should "download everything when forced, whatever is on disk" in {
    val dir  = tempDirWith("postgresql-42.7.11.jar")
    val plan = DependencySync.reconcile(java.util.List.of(pgArt), dir, sizes(pgUrl -> 10L), true)
    plan.getToDownload should have size 1
    plan.getUpToDate shouldBe empty
  }

  it should "treat a missing directory as empty rather than failing" in {
    val missing = new java.io.File(System.getProperty("java.io.tmpdir"), "sl-sync-does-not-exist-" + System.nanoTime())
    val plan    = DependencySync.reconcile(java.util.List.of(pgArt), missing, emptySizes, false)
    plan.getToDownload should have size 1
    plan.getToDelete shouldBe empty
  }

  it should "remove a superseded python wheel" in {
    // Regression test: the old code used the full versioned file name as the artefact name,
    // so a wheel bump never matched the previous wheel and python-libs grew without bound.
    val wheelUrl =
      "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_airflow-0.6.11-py3-none-any.whl"
    val wheel = artifact("Python libs", "starlake_airflow-0.6.11-py3-none-any.whl", wheelUrl)
    val dir   = tempDirWith("starlake_airflow-0.6.10-py3-none-any.whl")
    val plan  = DependencySync.reconcile(java.util.List.of(wheel), dir, emptySizes, false)
    plan.getToDelete should have size 1
    plan.getToDelete.get(0).file.getName shouldBe "starlake_airflow-0.6.10-py3-none-any.whl"
    plan.getToDownload should have size 1
  }

  it should "never let one artifact own another artifact's current file" in {
    // If this fails, syncing artifact A would classify artifact B's CURRENT file as
    // superseded and delete it. Fix by making the colliding prefix more specific, never by
    // relaxing the assertion.
    import scala.jdk.CollectionConverters._
    val artifacts = goldenPrefixes.map { case (url, _) =>
      val fileName = url.substring(url.lastIndexOf('/') + 1)
      artifact("golden", fileName, url)
    }
    val asJava = artifacts.asJava
    artifacts.foreach { a =>
      val owner = DependencySync.ownerOf(a.fileName, asJava)
      withClue(s"${a.fileName} is owned by ${owner.fileName}: ") {
        owner.fileName shouldBe a.fileName
      }
    }
  }
```

`ownerOf` is declared package-private (no modifier) in Step 3 below, so the test,
which lives in the same package, can call it.

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: compilation failure, `value reconcile is not a member of object DependencySync`.

- [ ] **Step 3: Implement `reconcile`**

Add to `src/main/java/ai/starlake/setup/DependencySync.java`, above the closing brace, and add the imports `java.io.File`, `java.util.HashSet`, `java.util.List`, `java.util.Map`, `java.util.Set` at the top:

```java
    /**
     * Diff the artifacts the installer wants against what {@code dir} already holds.
     *
     * @param artifacts   every artifact the installer knows about, ENABLED AND DISABLED
     *                    alike - the disabled ones are what let a turned-off category get
     *                    cleaned up instead of orphaned
     * @param dir         directory to reconcile; a directory that does not exist is treated
     *                    as empty
     * @param remoteSizes remote byte size per {@link Artifact#url}; a missing entry or a
     *                    value {@code <= 0} means the size could not be determined, and a
     *                    correctly named file is then kept (so a fully provisioned install
     *                    is an offline no-op instead of a hard failure)
     * @param force       classify every enabled artifact as a download, whatever is on disk
     */
    public static SyncPlan reconcile(List<Artifact> artifacts, File dir, Map<String, Long> remoteSizes, boolean force) {
        SyncPlan plan = new SyncPlan();

        Set<String> desiredNames = new HashSet<>();
        for (Artifact artifact : artifacts) {
            if (artifact.enabled) {
                desiredNames.add(artifact.fileName);
            }
        }

        File[] present = dir.listFiles();
        if (present == null) {
            present = new File[0];
        }

        for (Artifact artifact : artifacts) {
            if (!artifact.enabled) {
                continue;
            }
            File target = new File(dir, artifact.fileName);
            long remote = sizeOf(remoteSizes, artifact.url);
            boolean usable = !force
                    && target.isFile()
                    && (remote <= 0 || target.length() == remote);
            if (usable) {
                plan.addUpToDate(target);
            } else {
                plan.add(new SyncPlan.Download(artifact, target, remote));
            }
        }

        for (File file : present) {
            if (!file.isFile() || desiredNames.contains(file.getName())) {
                continue;
            }
            Artifact owner = ownerOf(file.getName(), artifacts);
            if (owner != null) {
                String reason = owner.enabled ? "superseded" : owner.label + " disabled";
                plan.add(new SyncPlan.Deletion(file, reason));
            }
        }
        return plan;
    }

    /**
     * The artifact a file on disk belongs to, or null when nothing manages it - a jar the
     * user hand-copied into bin/deps, which the installer must never touch.
     *
     * <p>Matched against the file NAME. The old code matched {@code File.getPath()}, so an
     * installation directory whose path happened to contain an artefact name made every
     * file in bin/deps a match and got it deleted.
     */
    static Artifact ownerOf(String fileName, List<Artifact> artifacts) {
        for (Artifact artifact : artifacts) {
            for (String prefix : artifact.ownershipPrefixes) {
                if (!prefix.isEmpty() && fileName.contains(prefix)) {
                    return artifact;
                }
            }
        }
        return null;
    }

    private static long sizeOf(Map<String, Long> remoteSizes, String url) {
        Long size = remoteSizes.get(url);
        return size == null ? -1L : size;
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: `Tests: succeeded 23, failed 0`.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/ai/starlake/setup/DependencySync.java src/test/scala/ai/starlake/setup/DependencySyncSpec.scala
git commit -m "feat(setup): reconcile desired dependencies against what is on disk"
```

---

## Task 6: Verify downloads against Content-Length

The plan now trusts on-disk sizes, so a truncated download must not be able to record itself as a valid size. This is also the standalone fix for a real defect: today a killed download is accepted silently and surfaces much later as a `ClassNotFoundException`.

**Files:**
- Modify: `src/main/java/Setup.java` (inside `downloadAndDisplayProgress(ResourceDependency, BiFunction)`)

**Interfaces:**
- Consumes: nothing.
- Produces: no new API; `downloadAndDisplayProgress` now throws `RuntimeException` on a short write.

- [ ] **Step 1: Add the length check after the copy loop**

In `src/main/java/Setup.java`, inside `downloadAndDisplayProgress(ResourceDependency resource, BiFunction<ResourceDependency, String, File> fileProducer)`, locate the block that ends the copy loop:

```java
                        for (int cnt = 0; cnt < sbLen; cnt++) {
                            System.out.print("\b");
                        }
                        System.out.print(file.getAbsolutePath() + " succesfully downloaded from " + urlFolder);
                        System.out.println();
                    }
                    succesfullyDownloaded = true;
                    break;
```

Replace it with:

```java
                        for (int cnt = 0; cnt < sbLen; cnt++) {
                            System.out.print("\b");
                        }
                        System.out.print(file.getAbsolutePath() + " succesfully downloaded from " + urlFolder);
                        System.out.println();
                        // A truncated download used to be accepted silently and only showed up much
                        // later as a ClassNotFoundException. It must fail here now for a second
                        // reason too: the sync plan decides a file is up to date from its size, so a
                        // short file would otherwise cache itself as valid forever.
                        if (lengthOfFile > 0 && total != lengthOfFile) {
                            downloadedBytes = total;
                            expectedBytes = lengthOfFile;
                        }
                    }
                    if (expectedBytes > 0) {
                        deleteFile(file);
                        throw new RuntimeException("Incomplete download of " + resource.artefactName + " from " + urlStr
                                + ": got " + downloadedBytes + " bytes, expected " + expectedBytes
                                + ". The partial file was removed; run the installation again.");
                    }
                    succesfullyDownloaded = true;
                    break;
```

Declare the two locals next to `boolean succesfullyDownloaded = false;` at the top of the method (the check cannot live inside the try-with-resources block, because the stream must be closed before `deleteFile` can succeed on Windows):

```java
            long downloadedBytes = 0;
            long expectedBytes = 0;
```

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: `[success]`.

- [ ] **Step 3: Verify the check fires on a truncated response**

Start a server that lies about `Content-Length`, then point one dependency at
it. Save this as `/tmp/liar.py`:

```python
import http.server

class Liar(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "999999")
        self.end_headers()
        self.wfile.write(b"short")

    def log_message(self, *args):
        pass

http.server.HTTPServer(("127.0.0.1", 8731), Liar).serve_forever()
```

The download URLs are compile-time constants, so redirect one of them for the
duration of the check. In `src/main/java/Setup.java` temporarily replace the
`POSTGRESQL_JAR` URL with `"http://127.0.0.1:8731/postgresql-42.7.11.jar"`, then:

```bash
python3 /tmp/liar.py &
LIAR=$!
TARGET=$(mktemp -d)
sbt packageSetup
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_POSTGRESQL=true ENABLE_API=false \
  java -cp distrib/setup.jar Setup "$TARGET" unix 2>&1 | grep -i incomplete
ls "$TARGET/bin/deps" | grep postgresql || echo "no partial file left, correct"
kill $LIAR
```

Expected: a line containing `Incomplete download of postgresql ... got 5 bytes, expected 999999`, followed by `no partial file left, correct`.

Then **revert the temporary URL change** before committing:

```bash
git diff src/main/java/Setup.java | grep 127.0.0.1 && echo "STILL PATCHED - revert before committing"
```

Do not skip this step: it is the only direct test of the integrity check, and every later task trusts on-disk sizes because of it.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/Setup.java
git commit -m "fix(setup): fail on a truncated download instead of accepting it silently"
```

---

## Task 7: Probe remote sizes

**Files:**
- Modify: `src/main/java/Setup.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `private static Map<String, Long> probeAll(List<Artifact> artifacts)` on `Setup`. Returns one entry per **enabled** artifact URL, value `-1` when the size could not be determined. Used by Tasks 8, 9 and 10.

- [ ] **Step 1: Add the imports**

At the top of `src/main/java/Setup.java`, add whichever of these are not already present:

```java
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import ai.starlake.setup.Artifact;
import ai.starlake.setup.DependencySync;
import ai.starlake.setup.SyncPlan;
```

- [ ] **Step 2: Add `probeAll` and `probeSize`**

Add these methods to `Setup`, next to `downloadAndDisplayProgress`:

```java
    /**
     * Remote byte size of every enabled artifact, keyed by url, -1 when unknown.
     *
     * <p>Runs concurrently over a small fixed pool so the whole probe costs roughly one
     * round trip rather than one per artifact. Never throws: a probe that fails yields -1,
     * which the reconciler reads as "keep whatever is on disk", making a fully provisioned
     * install an offline no-op.
     */
    private static Map<String, Long> probeAll(List<Artifact> artifacts) {
        Map<String, Long> sizes = new ConcurrentHashMap<>();
        List<Artifact> enabled = new ArrayList<>();
        for (Artifact artifact : artifacts) {
            if (artifact.enabled) {
                enabled.add(artifact);
            }
        }
        if (enabled.isEmpty()) {
            return sizes;
        }
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(8, enabled.size()));
        try {
            for (Artifact artifact : enabled) {
                pool.submit(() -> sizes.put(artifact.url, probeSize(artifact.url)));
            }
            pool.shutdown();
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
        return sizes;
    }

    /**
     * Remote byte size of one url, or -1.
     *
     * <p>HEAD first; some mirrors answer 405 or omit Content-Length, so fall back to a
     * one-byte ranged GET and read Content-Range. Uses the shared clientBuilder, so proxy,
     * authenticator, SL_INSECURE and redirect handling match the download path exactly.
     */
    private static long probeSize(String url) {
        try {
            HttpClient probeClient = clientBuilder.followRedirects(HttpClient.Redirect.ALWAYS).build();
            HttpRequest head = HttpRequest.newBuilder().uri(URI.create(url))
                    .method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
            HttpResponse<Void> headResponse = probeClient.send(head, HttpResponse.BodyHandlers.discarding());
            if (headResponse.statusCode() == 200) {
                long length = headResponse.headers().firstValueAsLong("Content-Length").orElse(-1L);
                if (length > 0) {
                    return length;
                }
            }
            HttpRequest ranged = HttpRequest.newBuilder().uri(URI.create(url))
                    .header("Range", "bytes=0-0").GET().build();
            HttpResponse<byte[]> rangedResponse = probeClient.send(ranged, HttpResponse.BodyHandlers.ofByteArray());
            if (rangedResponse.statusCode() == 206) {
                String contentRange = rangedResponse.headers().firstValue("Content-Range").orElse("");
                int slash = contentRange.lastIndexOf('/');
                if (slash >= 0 && slash < contentRange.length() - 1) {
                    return Long.parseLong(contentRange.substring(slash + 1).trim());
                }
            }
            return -1L;
        } catch (Exception e) {
            return -1L;
        }
    }
```

- [ ] **Step 3: Compile**

Run: `sbt compile`
Expected: `[success]`.

- [ ] **Step 4: Verify a probe against a real artifact**

```bash
cat > /tmp/ProbeCheck.java <<'EOF'
import java.net.URI;
import java.net.http.*;
public class ProbeCheck {
    public static void main(String[] a) throws Exception {
        HttpClient c = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();
        HttpRequest r = HttpRequest.newBuilder()
            .uri(URI.create("https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar"))
            .method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<Void> resp = c.send(r, HttpResponse.BodyHandlers.discarding());
        System.out.println(resp.statusCode() + " " + resp.headers().firstValue("Content-Length"));
    }
}
EOF
java /tmp/ProbeCheck.java
```

Expected: `200 Optional[1...]` with a non-zero length. This confirms Maven Central answers HEAD with a usable `Content-Length`; if it prints `405` or an empty Optional, the ranged-GET fallback is doing the work and Task 12's end-to-end run must confirm it.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/Setup.java
git commit -m "feat(setup): probe remote artifact sizes concurrently over HEAD"
```

---

## Task 8: Wire bin/deps to the reconciler

The core of the change: the 13 delete-then-download blocks become one declarative list, one plan, one apply.

**Files:**
- Modify: `src/main/java/Setup.java` (the `depsDir` section of `main`, currently lines 1109-1170)

**Interfaces:**
- Consumes: `Artifact`, `SyncPlan`, `DependencySync.reconcile`, `probeAll`.
- Produces: `private static List<Artifact> depsArtifacts()`, `private static List<Artifact> toArtifacts(String label, ResourceDependency[] deps, boolean enabled)`, and `private static void apply(SyncPlan plan)` on `Setup`. Used by Tasks 9, 10 and 11.

- [ ] **Step 1: Add the artifact builders and the apply step**

Add to `Setup`, above `main`:

```java
    /**
     * Turn a dependency category into plain Artifacts.
     *
     * <p>Two ownership prefixes per artifact. The DERIVED one (from the url) owns the
     * artifact's own superseded versions - it is the only one that matches for the AWS SDK,
     * whose label is "aws-java-sdk-bundle" but whose file is bundle-&lt;version&gt;.jar, so
     * without it every SDK bump leaked the previous bundle. The LEGACY one is today's
     * artefactName, kept because a few labels deliberately match a PREVIOUS major's file
     * name: "aws-java-sdk-bundle" cleans up the Spark 3 v1 jar, "bigquery-with-dependencies"
     * cleans up spark-bigquery-with-dependencies_2.13-*.jar.
     */
    private static List<Artifact> toArtifacts(String label, ResourceDependency[] deps, boolean enabled) {
        List<Artifact> artifacts = new ArrayList<>();
        for (ResourceDependency dep : deps) {
            String url = dep.urls[0];
            String fileName = dep.getUrlName(url);
            List<String> prefixes = new ArrayList<>();
            prefixes.add(DependencySync.derivePrefix(url, fileName));
            if (!prefixes.contains(dep.artefactName)) {
                prefixes.add(dep.artefactName);
            }
            artifacts.add(new Artifact(label, fileName, url, prefixes, enabled));
        }
        return artifacts;
    }

    /** Every artifact that belongs in bin/deps, enabled and disabled alike. */
    private static List<Artifact> depsArtifacts() {
        List<Artifact> all = new ArrayList<>();
        all.addAll(toArtifacts("Delta", deltaSparkDependencies, true));
        all.addAll(toArtifacts("Iceberg", icebergSparkDependencies, true));
        all.addAll(toArtifacts("DuckDB", duckDbDependencies, ENABLE_DUCKDB));
        all.addAll(toArtifacts("FlightSQL", flightSqlDependencies, ENABLE_FLIGHTSQL));
        all.addAll(toArtifacts("Kafka", confluentDependencies, ENABLE_KAFKA));
        all.addAll(toArtifacts("Redshift", redshiftDependencies, ENABLE_REDSHIFT));
        all.addAll(toArtifacts("BigQuery", bigqueryDependencies, ENABLE_BIGQUERY));
        all.addAll(toArtifacts("Azure", azureDependencies, ENABLE_AZURE));
        all.addAll(toArtifacts("Snowflake", snowflakeDependencies, ENABLE_SNOWFLAKE));
        all.addAll(toArtifacts("Postgres", postgresqlDependencies, ENABLE_POSTGRESQL));
        all.addAll(toArtifacts("Mariadb", mariadbDependencies, ENABLE_MARIADB));
        all.addAll(toArtifacts("Clickhouse", clickhouseDependencies, ENABLE_CLICKHOUSE));
        all.addAll(toArtifacts("Trino", trinodbDependencies, ENABLE_TRINODB));
        return all;
    }

    /** Deletions first, so a superseded jar is gone before its replacement lands. */
    private static void apply(SyncPlan plan) throws IOException, InterruptedException {
        for (SyncPlan.Deletion deletion : plan.getToDelete()) {
            deleteFile(deletion.file);
        }
        for (SyncPlan.Download download : plan.getToDownload()) {
            File parent = download.target.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            // fileName, not label: downloadAndDisplayProgress prints the artefact name, and
            // "Downloading postgresql-42.7.11.jar..." is what the user needs to see, not
            // "Downloading Postgres..." repeated once per jar in the category.
            downloadAndDisplayProgress(
                    new ResourceDependency(download.artifact.fileName, download.artifact.url),
                    (resource, url) -> download.target);
        }
    }
```

- [ ] **Step 2: Replace the 13 category blocks in `main`**

In `main`, delete every line from `deleteDependencies(deltaSparkDependencies, depsDir);` through `downloadAndDisplayProgress(trinodbDependencies, depsDir, true);` inclusive (the `updateSparkLog4j2Properties(sparkDir);` call sits in the middle of that run - keep it, move it above the new block). Replace with:

```java
            updateSparkLog4j2Properties(sparkDir);

            List<Artifact> deps = depsArtifacts();
            SyncPlan plan = DependencySync.reconcile(deps, depsDir, probeAll(deps), false);
            System.out.println(plan.render("Dependency plan for Starflow " + SL_VERSION + " (bin/deps)"));
            apply(plan);
```

- [ ] **Step 3: Compile**

Run: `sbt compile`
Expected: `[success]`. `deleteDependencies` is still referenced by `downloadAndDisplayProgress(..., replaceJar)`; leave both in place for now, Task 10 removes the last uses.

- [ ] **Step 4: Verify against a real install directory**

```bash
TARGET=$(mktemp -d)
sbt packageSetup
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_POSTGRESQL=true ENABLE_API=false \
  java -cp distrib/setup.jar Setup "$TARGET" unix
ls "$TARGET/bin/deps"
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_POSTGRESQL=true ENABLE_API=false \
  java -cp distrib/setup.jar Setup "$TARGET" unix
```

Expected: the first run prints a plan with `+ N to download` and populates `bin/deps`; the **second** run prints `All N dependencies up to date, nothing to download.` for the deps section and re-downloads no jar. If the second run still downloads, the probe is returning `-1` for those URLs (check with Task 7 Step 4) or the file names do not match.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/Setup.java
git commit -m "feat(setup): sync bin/deps from a diff instead of wiping and re-downloading"
```

---

## Task 9: Wire bin/sl

**Files:**
- Modify: `src/main/java/Setup.java` (the `slDir` section of `main`, currently lines 1083-1100)

**Interfaces:**
- Consumes: `toArtifacts`, `apply`, `probeAll`, `DependencySync.reconcile`.
- Produces: nothing new.

- [ ] **Step 1: Replace the unconditional wipe**

In `main`, replace:

```java
            File slDir = new File(binDir, "sl");
            deleteRecursively(slDir);
```

with:

```java
            File slDir = new File(binDir, "sl");
```

and replace the `else` branch:

```java
            } else {
                downloadAndDisplayProgress(new ResourceDependency[]{STARLAKE_RELEASE_JAR}, slDir, false);
            }
```

with:

```java
            } else {
                // No deleteRecursively here: the assembly is the single biggest download of the
                // whole install, and re-fetching an identical jar on every run is what made
                // `upgrade` feel like a fresh install. The reconciler still guarantees exactly one
                // assembly in bin/sl - any other starlake-core jar is classified superseded.
                // A non-jar file in bin/sl matches no ownership prefix and is left alone, which is
                // deliberately narrower than the wipe it replaces.
                List<Artifact> core = toArtifacts("Starflow core", new ResourceDependency[]{STARLAKE_RELEASE_JAR}, true);
                SyncPlan corePlan = DependencySync.reconcile(core, slDir, probeAll(core), false);
                System.out.println(corePlan.render("Core jar plan for Starflow " + SL_VERSION + " (bin/sl)"));
                apply(corePlan);
            }
```

Leave the `SL_CORE_JAR` branch above it untouched: it copies a locally built assembly, CI rebuilds it in place under the same name, and a local file copy costs nothing.

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: `[success]`.

- [ ] **Step 3: Verify a version switch leaves exactly one assembly**

```bash
TARGET=$(mktemp -d)
sbt packageSetup
SL_VERSION=1.8.2 ENABLE_ALL=false ENABLE_API=false java -cp distrib/setup.jar Setup "$TARGET" unix
ls "$TARGET/bin/sl"
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_API=false java -cp distrib/setup.jar Setup "$TARGET" unix
ls "$TARGET/bin/sl"
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_API=false java -cp distrib/setup.jar Setup "$TARGET" unix
```

Expected: after the second run `bin/sl` holds only `starlake-core_2.13-1.8.3-assembly.jar` (the 1.8.2 jar reported as `superseded` and removed); the third run reports the core jar up to date and downloads nothing.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/Setup.java
git commit -m "feat(setup): keep the core assembly when its version has not changed"
```

---

## Task 10: Wire python-libs and drop the old delete path

**Files:**
- Modify: `src/main/java/Setup.java` (`downloadPythonLibs`, plus removal of `deleteDependencies` and the `replaceJar` parameter)

**Interfaces:**
- Consumes: `Artifact`, `SyncPlan`, `DependencySync.reconcile`, `apply`, `probeAll`.
- Produces: nothing new. `deleteDependencies` and the `replaceJar` overload of `downloadAndDisplayProgress` are removed.

- [ ] **Step 1: Rewrite `downloadPythonLibs`**

Replace the body of `downloadPythonLibs` after the `versions.txt` read with a reconciled sync. The whole method becomes:

```java
    private static void downloadPythonLibs(File targetDir) throws IOException, InterruptedException {
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        // versions.txt IS the desired-state manifest, so it is always refetched.
        ResourceDependency versionsFile = new ResourceDependency("versions.txt", PYTHON_LIBS_URL + "versions.txt");
        downloadAndDisplayProgress(versionsFile, (resource, url) -> new File(targetDir, "versions.txt"));

        File versionsTxt = new File(targetDir, "versions.txt");
        if (!versionsTxt.exists()) {
            return;
        }
        List<String> filesToDownload = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(versionsTxt))) {
            String line;
            while ((line = br.readLine()) != null) {
                String trimmedLine = line.trim();
                if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("#")) {
                    filesToDownload.add(trimmedLine);
                }
            }
        }
        if (filesToDownload.isEmpty()) {
            return;
        }
        // Ownership must come from the wheel's DISTRIBUTION name, not its full file name.
        // Building a ResourceDependency whose artefactName was the versioned file name meant
        // the old cleanup only ever matched the identical name, so every wheel bump left the
        // previous version behind and python-libs grew without bound.
        List<Artifact> wheels = new ArrayList<>();
        for (String fileName : filesToDownload) {
            String url = PYTHON_LIBS_URL + fileName;
            wheels.add(new Artifact("Python libs", fileName, url,
                    java.util.Collections.singletonList(DependencySync.derivePrefix(url, fileName)), true));
        }
        SyncPlan plan = DependencySync.reconcile(wheels, targetDir, probeAll(wheels), false);
        System.out.println(plan.render("Python libs plan (bin/deps/python-libs)"));
        apply(plan);
    }
```

- [ ] **Step 2: Remove the now-dead delete path**

Delete the `deleteDependencies(ResourceDependency[], File)` method entirely. Then simplify the array overload of `downloadAndDisplayProgress`, whose `replaceJar` parameter now has no true call site:

```java
    private static void downloadAndDisplayProgress(ResourceDependency[] dependencies, File targetDir) throws IOException, InterruptedException {
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        for (ResourceDependency dependency : dependencies) {
            downloadAndDisplayProgress(dependency, (resource, url) -> new File(targetDir, resource.getUrlName(url)));
        }
    }
```

Update its three remaining call sites to drop the trailing boolean: the API zip in `downloadApi`, the Spark tarball in `downloadSpark`, and the `SL_CORE_JAR` branch if it still uses the array form.

- [ ] **Step 3: Compile**

Run: `sbt compile`
Expected: `[success]`, no "method deleteDependencies not found" and no unused-parameter lint.

- [ ] **Step 4: Verify a wheel bump removes the old wheel**

```bash
TARGET=$(mktemp -d)
sbt packageSetup
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_API=false java -cp distrib/setup.jar Setup "$TARGET" unix
ls "$TARGET/bin/deps/python-libs"
# simulate a stale wheel left by an older install
touch "$TARGET/bin/deps/python-libs/starlake_airflow-0.6.10-py3-none-any.whl"
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_API=false java -cp distrib/setup.jar Setup "$TARGET" unix
ls "$TARGET/bin/deps/python-libs"
```

Expected: the second run reports `starlake_airflow-0.6.10-py3-none-any.whl (superseded)` and removes it, while leaving `starlake_airflow-0.6.11-py3-none-any.whl` in place and downloading nothing.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/Setup.java
git commit -m "fix(setup): stop leaking superseded python wheels, drop the old delete path"
```

---

## Task 11: SL_FORCE_DOWNLOAD, SL_DRY_RUN, and the launcher flags

**Files:**
- Modify: `src/main/java/Setup.java`
- Modify: `distrib/starlake.sh`
- Modify: `distrib/starlake.cmd`

**Interfaces:**
- Consumes: `depsArtifacts`, `apply`, `probeAll`, `DependencySync.reconcile`.
- Produces: `SL_FORCE_DOWNLOAD` and `SL_DRY_RUN` honoured by `Setup`; `--force` and `--dry-run` accepted by `starlake install` and `starlake upgrade`.

- [ ] **Step 1: Read the two flags in Setup**

Add next to the other `ENABLE_*` fields in `src/main/java/Setup.java`:

```java
    // Force restores the pre-diff behaviour for one run: everything is re-downloaded whatever
    // is on disk. Probing is skipped entirely - there is no decision left to make, and the
    // download itself still fails loudly if the network is down.
    private static final boolean SL_FORCE_DOWNLOAD = envIsTrue("SL_FORCE_DOWNLOAD");
    private static final boolean SL_DRY_RUN = envIsTrue("SL_DRY_RUN");
```

- [ ] **Step 2: Thread them through the three reconciled sections**

In each of the three places that build a plan (`bin/deps` in `main`, `bin/sl` in `main`, `downloadPythonLibs`), replace the probe-and-reconcile pair with:

```java
            Map<String, Long> sizes = SL_FORCE_DOWNLOAD ? new java.util.HashMap<>() : probeAll(deps);
            SyncPlan plan = DependencySync.reconcile(deps, depsDir, sizes, SL_FORCE_DOWNLOAD);
```

using each section's own variable names. Then collect the three plans into one and print once, rather than printing three separate plans: declare `SyncPlan overall = new SyncPlan();` at the top of `main`, have each section call `overall.mergeFrom(plan)` instead of printing, and print `overall` once after the python-libs step:

```java
            System.out.println(overall.render("Dependency plan for Starflow " + SL_VERSION
                    + " (bin/deps, bin/sl, python-libs)"));
```

For that to work, apply must happen after the print, so restructure the three sections to only *build* their plan and merge it, then run one `apply(overall)` after the print.

- [ ] **Step 3: Honour dry-run**

Immediately after the `overall.render(...)` print, and before `apply(overall)`:

```java
            if (SL_DRY_RUN) {
                // Deliberately before generateVersions: versions.sh is what starlake.sh's
                // consistency check reads, so a dry run must not touch it.
                System.out.println("SL_DRY_RUN is set: nothing downloaded, nothing deleted.");
                return;
            }
            apply(overall);
```

- [ ] **Step 4: Add the launcher flags (bash)**

In `distrib/starlake.sh`, inside the `install|reinstall)` and `_do_upgrade)` cases, before the `launch_setup` call, add:

```bash
    # Both must be EXPORTED, not just set: launch_setup spawns `java -cp setup.jar Setup`
    # as a subprocess and a shell-local variable never reaches it - the same constraint
    # documented for the ENABLE_* flags at the top of this script.
    for arg in "$@"; do
      case "$arg" in
        --force)   export SL_FORCE_DOWNLOAD=true ;;
        --dry-run) export SL_DRY_RUN=true ;;
      esac
    done
```

- [ ] **Step 5: Add the launcher flags (cmd)**

In `distrib/starlake.cmd`, in the equivalent install and upgrade branches, before the Setup invocation:

```bat
:: Must be SET before the java call: Setup runs as a subprocess and reads these from the
:: environment, mirroring starlake.sh.
for %%A in (%*) do (
    if "%%A"=="--force" SET SL_FORCE_DOWNLOAD=true
    if "%%A"=="--dry-run" SET SL_DRY_RUN=true
)
```

- [ ] **Step 6: Compile and verify both flags**

```bash
sbt compile packageSetup
TARGET=$(mktemp -d)
SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_POSTGRESQL=true ENABLE_API=false \
  java -cp distrib/setup.jar Setup "$TARGET" unix
# dry run on a provisioned tree: prints the plan, changes nothing
BEFORE=$(find "$TARGET" -type f | sort | md5)
SL_DRY_RUN=true SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_POSTGRESQL=true ENABLE_API=false \
  java -cp distrib/setup.jar Setup "$TARGET" unix
AFTER=$(find "$TARGET" -type f | sort | md5)
[ "$BEFORE" = "$AFTER" ] && echo "DRY RUN OK" || echo "DRY RUN MODIFIED THE TREE"
# force: re-downloads everything
SL_FORCE_DOWNLOAD=true SL_VERSION=1.8.3 ENABLE_ALL=false ENABLE_POSTGRESQL=true ENABLE_API=false \
  java -cp distrib/setup.jar Setup "$TARGET" unix
```

Expected: `DRY RUN OK`, and the forced run prints `+ N to download` for every artifact rather than the up-to-date line. On Linux use `md5sum` instead of `md5`.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/Setup.java distrib/starlake.sh distrib/starlake.cmd
git commit -m "feat(setup): add SL_FORCE_DOWNLOAD and SL_DRY_RUN, with --force/--dry-run flags"
```

---

## Task 12: Regenerate setup.jar and verify end to end

**Files:**
- Modify: `distrib/setup.jar`

**Interfaces:**
- Consumes: everything above.
- Produces: a shippable installer.

- [ ] **Step 1: Run the whole reconciler spec**

Run: `sbt "testOnly ai.starlake.setup.DependencySyncSpec"`
Expected: `Tests: succeeded 23, failed 0`.

- [ ] **Step 2: Check formatting**

Run: `sbt scalafmtCheck`
Expected: `[success]`. CI runs this; a formatting failure blocks the branch.

- [ ] **Step 3: Rebuild the jar and confirm its contents**

```bash
sbt packageSetup
unzip -l distrib/setup.jar
```

Expected entries: `Setup.class`, `Setup$UserPwdAuth.class`, `Setup$ResourceDependency.class`, `ai/starlake/setup/Artifact.class`, `ai/starlake/setup/DependencySync.class`, `ai/starlake/setup/SyncPlan.class`, `ai/starlake/setup/SyncPlan$Download.class`, `ai/starlake/setup/SyncPlan$Deletion.class`. A missing `ai/starlake/setup/` prefix means Task 1 Step 3 did not take.

- [ ] **Step 4: Full end-to-end run against the real launcher**

```bash
TARGET=$(mktemp -d)/starlake
mkdir -p "$TARGET"
cp distrib/starlake.sh "$TARGET/starlake"
chmod +x "$TARGET/starlake"
time SL_VERSION=1.8.3 "$TARGET/starlake" install     # full provisioning, note the elapsed time
time SL_VERSION=1.8.3 "$TARGET/starlake" install     # expect the one-line up-to-date summary
time SL_VERSION=1.8.3 "$TARGET/starlake" upgrade --version 1.8.2
"$TARGET/starlake" --version
```

Expected: run 1 downloads everything; run 2 prints `All N dependencies up to date, nothing to download.` and finishes in seconds instead of minutes; run 3 downloads the core jar (plus any genuinely changed connector) and nothing else; the version check reports a working install. Record the three elapsed times in the PR description.

- [ ] **Step 5: Confirm reinstall still does a full wipe**

```bash
SL_VERSION=1.8.3 "$TARGET/starlake" reinstall
```

Expected: `bin/spark`, `bin/deps` and `bin/sl` are wiped and fully re-provisioned. `reinstall` is the "heal a poisoned install" hammer and must stay unconditional.

- [ ] **Step 6: Commit**

```bash
git add distrib/setup.jar
git commit -m "build: regenerate setup.jar with incremental dependency sync"
```

---

## Verification Summary

Before opening the PR, all of these must hold:

- `sbt "testOnly ai.starlake.setup.DependencySyncSpec"` passes with 23 tests.
- `sbt scalafmtCheck` passes.
- `unzip -l distrib/setup.jar` lists all eight class entries from Task 12 Step 3.
- A second `install` at the same version downloads nothing (Task 12 Step 4).
- An `upgrade` downloads only the core jar and genuinely changed connectors.
- `--dry-run` leaves the tree byte-identical, `versions.sh` included.
- `reinstall` still wipes and re-provisions.
