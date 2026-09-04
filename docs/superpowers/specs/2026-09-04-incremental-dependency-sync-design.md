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
3. **Truncated downloads are silently accepted.** Nothing compares bytes
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
SyncPlan reconcile(List<Managed> managed, File dir, SizeProbe probe)
```

Every file in `dir` and every desired artifact is classified:

| Bucket       | Rule                                                                                   |
|--------------|----------------------------------------------------------------------------------------|
| `upToDate`   | Desired, present under a desired name, size matches remote `Content-Length`             |
| `toDownload` | Desired, and either absent or present with a mismatched size                            |
| `toDelete`   | Present, name matches a known `artefactName`, not in the desired set                    |
| `ignored`    | Present, matches no known `artefactName` (hand-copied jars) - untouched, not reported   |

Ownership is decided by `artefactName` against `File.getName()`, scanning the
**full** category list *including disabled ones*. That is what makes turning
Snowflake off actually remove `snowflake-jdbc-*.jar` instead of orphaning it.

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
  one run. Probe failures are hard errors in this mode.
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
defect 3, and a precondition for trusting sizes in the plan.

## Testing

`Setup.java` is a default-package standalone file with no test coverage today.
The reconciler is the part worth testing and is pure given a desired list and a
directory listing, so `reconcile(...)` and the ownership predicate take plain
arguments: no static state, no network. Network probing sits behind a small
`SizeProbe` interface that the spec stubs.

New spec at `src/test/scala/ai/starlake/setup/SetupSyncSpec.scala`, driving the
reconciler against temp directories:

- fresh install (empty dir): everything in `toDownload`, nothing in `toDelete`
- no-op re-run: everything in `upToDate`, plan prints the single-line summary
- version bump: old jar in `toDelete`, new jar in `toDownload`
- category disabled: its jars in `toDelete`, nothing downloaded for it
- hand-copied unknown jar: classified `ignored`, never deleted
- stale python wheel: older version in `toDelete` (regression test for defect 2)
- size mismatch on a correctly named file: forced into `toDownload`
- probe failure on a present file: classified `upToDate` (offline no-op)
- probe failure under `SL_FORCE_DOWNLOAD`: hard error
- install dir path containing an artefact name: no spurious deletions
  (regression test for defect 1)

Manual verification before this is considered done:

1. `install` into a temp dir, note the total download size.
2. Re-run `install` at the same version: expect
   `All N dependencies up to date, nothing to download.`
3. `upgrade` to a different version: expect only the core jar (plus any
   genuinely changed connector) in the download list.
4. `install --dry-run` on a dirty tree: plan printed, `bin/` and `versions.sh`
   unmodified.
