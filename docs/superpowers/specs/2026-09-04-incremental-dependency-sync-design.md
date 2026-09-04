# Incremental dependency sync: diff instead of re-download on upgrade

Date: 2026-09-04
Status: Approved

## Goal

`starlake upgrade` (and a repeated `starlake install`) must download only what
is actually missing or out of date. Today every dependency is deleted and
re-fetched on every run, so an upgrade that changes nothing but the core jar
still pulls roughly 1.5 GB.

`Setup.java` computes a desired state, diffs it against what is on disk, prints
the plan, and applies only the difference.

## Current state (before)

`src/main/java/Setup.java` `main()` runs 13 near-identical category blocks:

```java
deleteDependencies(snowflakeDependencies, depsDir);
if (ENABLE_SNOWFLAKE) {
    downloadAndDisplayProgress(snowflakeDependencies, depsDir, true);
}
```

- `deleteDependencies` wipes every file whose path contains the artefact name.
- `downloadAndDisplayProgress(..., replaceJar = true)` calls
  `deleteDependencies` a second time, then downloads unconditionally.
- There is no existence check anywhere in the deps path.

Beyond `bin/deps`:

- `bin/sl` is `deleteRecursively`'d and the core assembly re-downloaded even
  when `SL_VERSION` is unchanged (`Setup.java:1083`).
- `downloadPythonLibs` re-downloads every wheel listed in `versions.txt` on
  every run.
- `bin/spark` is already incremental (`if (!sparkDir.exists())`) and stays as
  it is.
- `bin/api` is re-fetched and re-extracted every run and stays as it is.

`starlake.sh _do_upgrade` does not touch `bin/deps` itself; it relies entirely
on `Setup.java` (see the comment at `starlake.sh:678`). `starlake.sh reinstall`
does `rm -rf bin/spark bin/deps bin/sl` before launching Setup.

Since `Setup.java` drives Unix, Windows and Docker installs alike, the whole
change lands in one file.

## Defects found in the current code, fixed here

1. **`deleteDependencies` matches on `File.getPath()`, not `getName()`.** An
   install directory whose path contains an artefact name (e.g.
   `/opt/postgresql/starlake`) makes every file in `bin/deps` match and get
   deleted. Fixed by matching on `getName()`.
2. **Stale python wheels accumulate forever.** `downloadPythonLibs` builds each
   `ResourceDependency` with the *full versioned filename* as `artefactName`,
   so `deleteDependencies` only ever matches the identical name. Upgrading
   `starlake_airflow` 0.6.10 to 0.6.11 leaves both wheels in
   `bin/deps/python-libs`. Fixed by deriving ownership from the wheel's
   distribution name.
3. **Stale AWS SDK v2 bundles accumulate.** `AWS_JAVA_SDK_JAR` carries the
   artefact label `aws-java-sdk-bundle` (deliberately, to clean up the legacy
   Spark 3 v1 jar - see the comment at `Setup.java:514`), but the file it
   actually downloads is `bundle-<version>.jar`, which that label does not
   match. Every AWS SDK bump therefore leaves the previous `bundle-*.jar`
   behind. Fixed by the derived ownership prefix in Part 2.
4. **Truncated downloads are silently accepted.** Nothing compares bytes
   written against `Content-Length`; a killed download surfaces later as a
   `ClassNotFoundException`. This must be fixed here, because the plan now
   trusts on-disk sizes: without it a truncated file would be cached as up to
   date by its own recorded size.

## Decisions

- **Identity signal: name + size.** A present file is up to date when its name
  is in the desired set *and* its byte size matches the remote
  `Content-Length`. No checksum sidecars (GitHub Releases, Confluent and the
  Apache mirrors do not publish them uniformly) and no local manifest (new
  state that drifts when users hand-copy jars, a workflow the installer itself
  advertises).
- **Scope: `bin/deps`, `bin/sl`, `bin/deps/python-libs`.** `bin/spark` and
  `bin/api` are unchanged.
- **Deletions cover managed artifacts only.** A file is deleted only when its
  name matches a known `artefactName` and it is not in the desired set.
  Unknown files are left untouched and not reported.
- **`starlake reinstall` keeps its full-wipe semantics.** After this change it
  is genuinely distinct from `upgrade` rather than an accidental synonym.

## Part 1: Desired state

Introduce a record pairing each category with its enable flag:

```java
record Managed(String label, ResourceDependency[] deps, boolean enabled) {}
```

(Records are available: Java sources compile with `--release 17` as of
`bc0a995c4`.)

The 13 delete/download blocks in `main()` collapse into one list built from the
existing `ENABLE_*` fields. `label` is the user-facing category name used in
the plan output ("Snowflake disabled"). `deltaSparkDependencies` and
`icebergSparkDependencies` are listed with `enabled = true`, matching today's
unconditional behavior.

Desired filename for a resource is `dep.getUrlName(urls[0])`. Every
`ResourceDependency` in the deps set has exactly one URL today; where a
resource has several, the remaining URL names count as *acceptable if already
present*, so adding a mirror later does not force a re-download.

## Part 2: Reconciliation

```java
SyncPlan reconcile(List<Artifact> artifacts, File dir, Map<String, Long> remoteSizes, boolean force)
```

`reconcile` is pure: plain data in, plan out, no static state and no network.
Remote sizes are probed separately by `probeAll(...)` and passed in as a map
(`-1` meaning "could not be determined"), so the reconciler needs no injected
probe interface and the test needs no stub.

Every file in `dir` and every desired artifact is classified:

| Bucket       | Rule                                                                                   |
|--------------|----------------------------------------------------------------------------------------|
| `upToDate`   | Desired, present under a desired name, size matches remote `Content-Length`             |
| `toDownload` | Desired, and either absent or present with a mismatched size                            |
| `toDelete`   | Present, name matches a known `artefactName`, not in the desired set                    |
| `ignored`    | Present, matches no known `artefactName` (hand-copied jars) - untouched, not reported   |

Ownership is decided against `File.getName()` (never `getPath()`), scanning the
**full** category list *including disabled ones*. That is what makes turning
Snowflake off actually remove `snowflake-jdbc-*.jar` instead of orphaning it.

A file is owned when its name contains **either** of two prefixes:

- the **derived prefix**, computed from the artifact's own URL as the file name
  truncated at the version, where the version is the second-to-last URL path
  segment with any leading `v` stripped (`.../bundle/2.29.52/bundle-2.29.52.jar`
  yields `bundle-`). When that segment does not appear in the file name (python
  wheels, winutils), the fallback is the name up to the first `-` followed by a
  digit (`starlake_airflow-`), else the whole name.
- the **legacy label**, i.e. today's `artefactName`. Retained so the deliberate
  cross-major cleanups keep working: `aws-java-sdk-bundle` still removes the
  Spark 3 v1 jar, and `bigquery-with-dependencies` still removes the old
  `spark-bigquery-with-dependencies_2.13-*.jar`.

The derived prefix is what fixes defect 3; the legacy label alone never matched
`bundle-*.jar`. Two invariants are enforced by test: no artifact's desired file
name may be owned by a *different* artifact, and the current desired names are
excluded from `toDelete` before deletion runs.

### Size probing

One `HEAD` per desired artifact, issued concurrently across a small fixed
thread pool so the probe costs about one round trip rather than 40. `HEAD`
reuses the existing `clientBuilder`, inheriting proxy, authenticator,
`SL_INSECURE` and `followRedirects(ALWAYS)`, so corporate-proxy installs behave
exactly as they do now.

Fallbacks:

- `HEAD` rejected (405) or no `Content-Length` returned: retry once as a ranged
  `GET` (`Range: bytes=0-0`, read `Content-Range`). If that also yields nothing,
  a name match counts as up to date.
- Probe fails for a **missing** local file: still queued for download, the plan
  shows an unknown size.
- Probe fails for a **present** local file (network unreachable): treated as up
  to date. This is a deliberate behavior change - it makes a fully provisioned
  install a genuine offline no-op instead of a hard failure. The cost is that a
  broken network reports "everything up to date" rather than an error;
  `SL_FORCE_DOWNLOAD` still fails loudly in that case.

## Part 3: Applying

Order is **delete, then download**, so a stale `postgresql-42.7.10.jar` is gone
before `42.7.11` lands and a half-applied plan never leaves two versions of one
driver on the classpath.

The same reconciler runs over three roots:

- **`bin/deps`** - replaces both the per-category `deleteDependencies` call and
  the `replaceJar = true` flag, which currently deletes the very files it is
  about to re-fetch.
- **`bin/sl`** - desired set is exactly one jar,
  `starlake-core_$SCALA_VERSION-$SL_VERSION-assembly.jar`. The unconditional
  `deleteRecursively(slDir)` at `Setup.java:1083` is removed; any *other* jar in
  `bin/sl` lands in `toDelete`, so a version switch still leaves precisely one
  assembly. A non-jar file in `bin/sl` matches no `artefactName` and is
  therefore `ignored` rather than deleted - a deliberate narrowing of today's
  `deleteRecursively`, which wipes the directory wholesale.
  The `SL_CORE_JAR` branch (locally built core, used by CI docker
  builds and local dev) keeps copying unconditionally: it is a local file copy
  and CI rebuilds it in place under the same name.
- **`bin/deps/python-libs`** - `versions.txt` is still fetched on every run,
  since it *is* the desired-state manifest. Ownership is derived from the
  wheel's distribution name (`starlake_airflow` from
  `starlake_airflow-0.6.11-py3-none-any.whl`, i.e. the substring before the
  first `-`) rather than the full filename. This is the fix for defect 2.

`bin/spark`, `bin/api` and `bin/hadoop` (Windows winutils) keep their current
handling.

## Part 4: Output and flags

The three roots are reconciled first, their plans merged, and the merged plan
printed once before anything is touched:

```
Dependency plan for Starflow 1.8.4 (bin/deps, bin/sl, python-libs)
  = 36 up to date
  + 2 to download (412 MB)
      starlake-core_2.13-1.8.4-assembly.jar    (398 MB)
      starlake_airflow-0.6.11-py3-none-any.whl  (14 MB)
  - 3 to remove
      starlake-core_2.13-1.8.3-assembly.jar     (superseded)
      starlake_airflow-0.6.10-py3-none-any.whl  (superseded)
      snowflake-jdbc-4.3.3.jar                  (Snowflake disabled)
```

With nothing to do it collapses to a single line:

```
All 41 dependencies up to date, nothing to download.
```

Per-file lines appear only for changes. Up-to-date files are a count only - the
common no-op path stays one line rather than 40.

Flags:

- **`SL_FORCE_DOWNLOAD=true`** - every desired artifact is classified
  `toDownload` regardless of what is on disk, reproducing today's behavior for
  one run. Probing is skipped entirely in this mode: there is no decision left
  to make, so a failed probe would only cost a round trip and an unknown size
  in the printed plan. The download itself still fails loudly on a network
  problem.
- **`SL_DRY_RUN=true`** - print the plan and exit 0 before any delete or
  download. `versions.sh` / `versions.cmd` is **not** rewritten in this mode,
  which matters because it is the file `starlake.sh`'s consistency check reads.
- **`starlake install --dry-run` / `--force`** in `starlake.sh` and
  `starlake.cmd` map to those two env vars. `launch_setup` already spawns Setup
  as a subprocess, so both must be `export`ed, not merely set (same constraint
  documented for the `ENABLE_*` flags at `starlake.sh:26`).
- **`starlake reinstall`** is untouched and keeps
  `rm -rf bin/spark bin/deps bin/sl`.

### Download integrity

`downloadAndDisplayProgress` compares bytes written against `Content-Length` on
completion; on mismatch it deletes the partial file and fails. Required by
defect 4, and a precondition for trusting sizes in the plan.

## Testing

`Setup.java` lives in the **unnamed package**, which no test in a named package
can reference. Rather than write the test in the unnamed package too, the pure
core moves into a real package: `ai.starlake.setup.DependencySync` and
`ai.starlake.setup.SyncPlan`, both plain Java (they ship inside `setup.jar`,
which runs as `java -cp setup.jar Setup` with no Scala library on the
classpath). `Setup.java` imports them - importing *from* a named package *into*
the unnamed package is legal Java, only the reverse is forbidden.

The bridge type is plain data, so the reconciler never sees
`ResourceDependency`:

```java
public final class Artifact {
    public final String label;                  // category label, for the plan output
    public final String fileName;               // desired file name
    public final String url;
    public final List<String> ownershipPrefixes;
    public final boolean enabled;
}
```

This forces a change to `packageSetup` in `build.sbt`, which today packages a
hardcoded list of exactly three class files (`Setup.class`,
`Setup$UserPwdAuth.class`, `Setup$ResourceDependency.class`) flattened to the
jar root by `IO.jar(... -> f.getName())`. Any new nested class is silently
omitted and fails at runtime with `NoClassDefFoundError`. The task list starts
by replacing that list with a directory scan that preserves relative paths.

New spec at `src/test/scala/ai/starlake/setup/DependencySyncSpec.scala`, driving
the reconciler against temp directories:

- fresh install (empty dir): everything in `toDownload`, nothing in `toDelete`
- no-op re-run: everything in `upToDate`, plan prints the single-line summary
- version bump: old jar in `toDelete`, new jar in `toDownload`
- category disabled: its jars in `toDelete`, nothing downloaded for it
- hand-copied unknown jar: classified `ignored`, never deleted
- stale python wheel: older version in `toDelete` (regression test for defect 2)
- size mismatch on a correctly named file: forced into `toDownload`
- probe failure on a present file (size `-1`): classified `upToDate` (offline
  no-op)
- `force = true`: everything in `toDownload` regardless of disk state
- install dir path containing an artefact name: no spurious deletions
  (regression test for defect 1)
- stale `bundle-*.jar` removed while the legacy `aws-java-sdk-bundle-*.jar`
  cleanup still fires (regression test for defect 3)
- ownership prefix derivation asserted for all 28 declared dependencies, plus
  the invariant that no artifact's desired name is owned by another artifact

Manual verification before this is considered done:

1. `install` into a temp dir, note the total download size.
2. Re-run `install` at the same version: expect
   `All N dependencies up to date, nothing to download.`
3. `upgrade` to a different version: expect only the core jar (plus any
   genuinely changed connector) in the download list.
4. `install --dry-run` on a dirty tree: plan printed, `bin/` and `versions.sh`
   unmodified.
