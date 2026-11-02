#!/usr/bin/env bats
#
# Runs the real distrib/starlake.sh in a throwaway install tree. Only the process
# boundaries are stubbed: spark-submit, java and curl.
#
# Run with: bats src/test/shell/starlake_appcds.bats

LAUNCHER="$BATS_TEST_DIRNAME/../../../distrib/starlake.sh"

make_install() {
  ROOT="$1"
  mkdir -p "$ROOT/bin/spark/bin" "$ROOT/bin/spark/jars" "$ROOT/bin/spark/conf" \
    "$ROOT/bin/deps" "$ROOT/bin/sl" "$ROOT/jdk/bin"
  cp "$LAUNCHER" "$ROOT/starlake.sh"
  chmod +x "$ROOT/starlake.sh"
  cat >"$ROOT/versions.sh" <<'EOF'
SL_VERSION=${SL_VERSION:-9.9.9}
SCALA_VERSION=${SCALA_VERSION:-2.13}
SPARK_VERSION=${SPARK_VERSION:-4.1.3}
HADOOP_VERSION=${HADOOP_VERSION:-3}
EOF
  touch "$ROOT/bin/spark/jars/spark-core_2.13-4.1.3.jar" \
    "$ROOT/bin/deps/dep.jar" \
    "$ROOT/bin/sl/starlake-core_2.13-9.9.9-assembly.jar" \
    "$ROOT/bin/spark/conf/log4j2.properties" \
    "$ROOT/bin/spark/conf/spark-defaults.conf.template"
  echo 'IMPLEMENTOR_VERSION="stub-17"' >"$ROOT/jdk/release"

  # Records one tab-separated line per call. Dumps an archive the way the JVM does.
  cat >"$ROOT/bin/spark/bin/spark-submit" <<'EOF'
#!/usr/bin/env bash
opts=""; master=""; args=("$@")
for ((i = 0; i < $#; i++)); do
  case "${args[$i]}" in
    --driver-java-options) opts="${args[$((i + 1))]}" ;;
    --master) master="${args[$((i + 1))]}" ;;
  esac
done
printf 'SUBMIT\topts=%s\tmaster=%s\tconf_dir=%s\tsl_env=%s\tsl_root=%s\tmemory=%s\tnice=%s\tconf_options=%s\n' \
  "$opts" "$master" "${SPARK_CONF_DIR:-}" "${SL_ENV:-}" "${SL_ROOT:-}" "${SPARK_DRIVER_MEMORY:-}" "$(nice)" "${SPARK_CONF_OPTIONS:-}" >>"$STUB_LOG"
[ -n "${STUB_SUBMIT_EXIT:-}" ] && exit "$STUB_SUBMIT_EXIT"
case "$opts" in
  *-XX:ArchiveClassesAtExit=*)
    touch derby.log
    if [ -n "${STUB_TRAINING_GATE:-}" ]; then
      for _ in $(seq 1 200); do [ -e "$STUB_TRAINING_GATE" ] && break; sleep 0.1; done
    fi
    file="${opts##*-XX:ArchiveClassesAtExit=}"
    [ -n "${STUB_NO_DUMP:-}" ] || echo archive >"${file%% *}"
    ;;
esac
exit "${STUB_EXIT_AFTER_DUMP:-0}"
EOF
  cat >"$ROOT/jdk/bin/java" <<'EOF'
#!/usr/bin/env bash
printf 'JAVA\t%s\n' "$*" >>"$STUB_LOG"
exit 0
EOF
  chmod +x "$ROOT/bin/spark/bin/spark-submit" "$ROOT/jdk/bin/java"
}

setup() {
  export STUB_LOG="$BATS_TEST_TMPDIR/calls.log"
  : >"$STUB_LOG"
  INSTALL="$BATS_TEST_TMPDIR/install"
  make_install "$INSTALL"
  export JAVA_HOME="$INSTALL/jdk"

  mkdir -p "$BATS_TEST_TMPDIR/path" "$BATS_TEST_TMPDIR/cwd"
  # The downloaded Setup.java pins the installed Spark, so sync_spark_runtime keeps it.
  cat >"$BATS_TEST_TMPDIR/path/curl" <<'EOF'
#!/usr/bin/env bash
while [ $# -gt 0 ]; do
  [ "$1" = "-o" ] && echo 'getEnv("SPARK_VERSION").orElse("4.1.3")' >"$2"
  shift
done
printf 200
EOF
  # A launcher resolved through PATH must never reach a real install of this machine,
  # whose archive a test would rebuild with the stubs.
  cat >"$BATS_TEST_TMPDIR/path/starlake.sh" <<'EOF'
#!/usr/bin/env bash
printf 'PATH_LAUNCHER\t%s\n' "$*" >>"$STUB_LOG"
EOF
  chmod +x "$BATS_TEST_TMPDIR/path/curl" "$BATS_TEST_TMPDIR/path/starlake.sh"
  export PATH="$BATS_TEST_TMPDIR/path:$PATH"
  cd "$BATS_TEST_TMPDIR/cwd"

  unset SPARK_CONF_DIR SPARK_DRIVER_OPTIONS SPARK_MASTER_URL SPARK_CONF_OPTIONS \
    SL_APPCDS SL_ENV SL_ROOT SL_RUN_MODE SL_DRY_RUN SL_DEBUG SL_HTTP_PORT HTTP_PROXY HTTPS_PROXY
  export -n JAVA_OPTS 2>/dev/null || true
  unset JAVA_OPTS
}

teardown() {
  [ -n "${STUB_TRAINING_GATE:-}" ] && touch "$STUB_TRAINING_GATE"
  wait_for_background_builds
}

archive() { echo "$INSTALL/.appcds/starlake.jsa"; }
# Background builds append to the same log, so commands and trainings are read apart.
last_submit() { grep '^SUBMIT' "$STUB_LOG" | grep -v 'ArchiveClassesAtExit' | tail -1; }
last_training() { grep '^SUBMIT' "$STUB_LOG" | grep 'ArchiveClassesAtExit' | tail -1; }
training_count() { grep -c 'ArchiveClassesAtExit' "$STUB_LOG" || true; }

wait_for_background_builds() {
  for _ in $(seq 1 100); do
    pgrep -f "$BATS_TEST_TMPDIR/.*starlake.sh appcds-build" >/dev/null || return 0
    sleep 0.1
  done
  return 1
}

wait_for_training_count() {
  for _ in $(seq 1 100); do
    [ "$(training_count)" -ge "$1" ] && return 0
    sleep 0.1
  done
  return 1
}

wait_for_rebuild() {
  wait_for_training_count "$1"
  wait_for_background_builds
}

wait_for_lock_release() {
  for _ in $(seq 1 100); do
    [ ! -e "$INSTALL/.appcds/build.lock" ] && return 0
    sleep 0.1
  done
  return 1
}

# Long enough for a wrongly started build to reach the stub.
assert_no_new_training() {
  sleep 1
  [ "$(training_count)" -eq "$1" ]
}

assert_cds_active() {
  [[ "$(last_submit)" == *"-XX:SharedArchiveFile=$(archive)"* ]]
  [[ "$(last_submit)" == *"conf_dir=$INSTALL/.appcds/conf"* ]]
  [[ "$(last_submit)" == *"-Dlog4j.configurationFile=$INSTALL/bin/spark/conf/log4j2.properties"* ]]
}

assert_cds_inactive() {
  [[ "$(last_submit)" != *"SharedArchiveFile"* ]]
  [[ "$(last_submit)" != *"conf_dir=$INSTALL/.appcds/conf"* ]]
}

# JVM flag merging

@test "proxy flags reach the driver when the user also sets SPARK_DRIVER_OPTIONS" {
  HTTP_PROXY=http://proxy.example:3128 SPARK_DRIVER_OPTIONS="-Dmine=1" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(last_submit)" == *"-Dhttp.proxyHost=proxy.example"* ]]
  [[ "$(last_submit)" == *"-Dhttp.proxyPort=3128"* ]]
  [[ "$(last_submit)" == *"-Dmine=1"* ]]
}

@test "user driver options come after launcher flags so they win" {
  SPARK_DRIVER_OPTIONS="-Dmine=1" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(last_submit)" == *"-Djava.net.preferIPv6Addresses=system -Dmine=1"* ]]
}

# Building the archive

@test "appcds-build trains spark-submit with an empty conf dir and publishes the archive" {
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ -s "$(archive)" ]
  [ -z "$(ls -A "$INSTALL/.appcds/conf")" ]
  [[ "$(last_training)" == *"conf_dir=$INSTALL/.appcds/conf"* ]]
}

@test "training ignores the caller environment" {
  SL_ENV=PROD SPARK_MASTER_URL=spark://cluster:7077 SPARK_CONF_OPTIONS="--conf a=b" SPARK_CONF_DIR=/elsewhere \
    SPARK_DRIVER_OPTIONS="-Dmine=1" SPARK_DRIVER_MEMORY=64g run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [[ "$(last_training)" == *"master=local[*]"* ]]
  [[ "$(last_training)" == *"sl_env="$'\t'* ]]
  [[ "$(last_training)" == *"memory=1g"* ]]
  [[ "$(last_training)" == *"conf_options=" ]]
  [[ "$(last_training)" != *"-Dmine=1"* ]]
}

@test "training leaves nothing behind in the working directory or the temp project" {
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ -z "$(ls -A "$BATS_TEST_TMPDIR/cwd")" ]
  training_root="$(last_training | sed -E 's/.*sl_root=([^\t]*).*/\1/')"
  [ -n "$training_root" ]
  [ ! -e "$training_root" ]
}

@test "a failed training exits non-zero and publishes no archive" {
  STUB_SUBMIT_EXIT=1 run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -ne 0 ]
  [ ! -e "$(archive)" ]
}

@test "a training that fails after dumping publishes no archive" {
  STUB_EXIT_AFTER_DUMP=1 run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -ne 0 ]
  [ ! -e "$(archive)" ]
  [ ! -e "$(archive).tmp" ]
}

@test "a training that succeeds without dumping reports a failure" {
  STUB_NO_DUMP=1 run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -ne 0 ]
  [ ! -e "$(archive)" ]
}

@test "a failed rebuild keeps the previous archive usable" {
  "$INSTALL/starlake.sh" appcds-build
  STUB_NO_DUMP=1 run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -ne 0 ]
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_active
}

@test "a manual build does nothing while another build runs" {
  "$INSTALL/starlake.sh" appcds-build
  mkdir "$INSTALL/.appcds/build.lock"
  : >"$STUB_LOG"
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ "$(training_count)" -eq 0 ]
  [ -d "$INSTALL/.appcds/build.lock" ]
}

@test "a build releases its lock" {
  run "$INSTALL/starlake.sh" appcds-build
  [ ! -e "$INSTALL/.appcds/build.lock" ]
  STUB_NO_DUMP=1 run "$INSTALL/starlake.sh" appcds-build
  [ ! -e "$INSTALL/.appcds/build.lock" ]
}

@test "appcds-build with SL_APPCDS=false does nothing" {
  SL_APPCDS=false run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ ! -e "$(archive)" ]
  [ -z "$(last_submit)" ]
}

# Using the archive

@test "no archive means no AppCDS and no warning" {
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
  [[ "$output" != *"appcds-build"* ]]
}

@test "a fresh archive is used on local master" {
  "$INSTALL/starlake.sh" appcds-build
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_active
  [[ "$output" != *"appcds-build"* ]]
}

@test "user driver options win over the AppCDS logging flag" {
  "$INSTALL/starlake.sh" appcds-build
  SPARK_DRIVER_OPTIONS="-Dlog4j.configurationFile=/mine.properties" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(last_submit)" == *"$INSTALL/bin/spark/conf/log4j2.properties"*"-Dlog4j.configurationFile=/mine.properties"* ]]
}

@test "SL_APPCDS=false disables it" {
  "$INSTALL/starlake.sh" appcds-build
  SL_APPCDS=false run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "a non-local master does not use it" {
  "$INSTALL/starlake.sh" appcds-build
  SPARK_MASTER_URL=spark://cluster:7077 run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "SL_RUN_MODE=main does not use it" {
  "$INSTALL/starlake.sh" appcds-build
  : >"$STUB_LOG"
  SL_RUN_MODE=main run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  grep -q '^JAVA.*ai.starlake.job.Main validate' "$STUB_LOG"
  ! grep -q 'SharedArchiveFile' "$STUB_LOG"
}

@test "spark config beyond logging disables it so the config is not ignored" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/spark/conf/spark-defaults.conf"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "a user SPARK_CONF_DIR is respected and disables it" {
  "$INSTALL/starlake.sh" appcds-build
  SPARK_CONF_DIR=/my/conf run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(last_submit)" == *"conf_dir=/my/conf"* ]]
  [[ "$(last_submit)" != *"SharedArchiveFile"* ]]
}

@test "user CDS flags are not overridden" {
  "$INSTALL/starlake.sh" appcds-build
  SPARK_DRIVER_OPTIONS="-XX:SharedArchiveFile=/mine.jsa" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(last_submit)" == *"-XX:SharedArchiveFile=/mine.jsa"* ]]
  [[ "$(last_submit)" != *"-XX:SharedArchiveFile=$(archive)"* ]]
}

@test "an added jar makes the archive stale" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "a removed jar makes the archive stale" {
  touch "$INSTALL/bin/deps/dep2.jar"
  "$INSTALL/starlake.sh" appcds-build
  rm "$INSTALL/bin/deps/dep2.jar"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "a replaced jar with the same name makes the archive stale" {
  "$INSTALL/starlake.sh" appcds-build
  touch -t 203001010000 "$INSTALL/bin/deps/dep.jar"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "a changed JDK makes the archive stale" {
  "$INSTALL/starlake.sh" appcds-build
  echo 'IMPLEMENTOR_VERSION="stub-21"' >"$INSTALL/jdk/release"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "a moved install makes the archive stale" {
  "$INSTALL/starlake.sh" appcds-build
  mv "$INSTALL" "$INSTALL-moved"
  JAVA_HOME="$INSTALL-moved/jdk" run "$INSTALL-moved/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(last_submit)" != *"SharedArchiveFile"* ]]
}

@test "an install path with a space is skipped because driver options cannot quote it" {
  SPACED="$BATS_TEST_TMPDIR/my install"
  make_install "$SPACED"
  JAVA_HOME="$SPACED/jdk" run "$SPACED/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ ! -e "$SPACED/.appcds/starlake.jsa" ]
  [ -z "$(last_submit)" ]
}

# Install and upgrade

@test "install builds the archive" {
  run "$INSTALL/starlake.sh" install
  [ "$status" -eq 0 ]
  [ -s "$(archive)" ]
}

@test "reinstall builds the archive" {
  # reinstall wipes bin/spark, bin/deps and bin/sl; the Setup stub restores them.
  cat >"$INSTALL/jdk/bin/java" <<EOF
#!/usr/bin/env bash
mkdir -p "$INSTALL/bin/spark/jars" "$INSTALL/bin/spark/conf" "$INSTALL/bin/spark/bin" "$INSTALL/bin/deps" "$INSTALL/bin/sl"
touch "$INSTALL/bin/spark/jars/spark-core_2.13-4.1.3.jar" "$INSTALL/bin/deps/dep.jar" "$INSTALL/bin/sl/starlake-core_2.13-9.9.9-assembly.jar"
cp "$BATS_TEST_TMPDIR/spark-submit" "$INSTALL/bin/spark/bin/spark-submit"
cp "$BATS_TEST_TMPDIR/versions.sh" "$INSTALL/versions.sh"
EOF
  cp "$INSTALL/bin/spark/bin/spark-submit" "$BATS_TEST_TMPDIR/spark-submit"
  cp "$INSTALL/versions.sh" "$BATS_TEST_TMPDIR/versions.sh"
  run "$INSTALL/starlake.sh" reinstall
  [ "$status" -eq 0 ]
  [ -s "$(archive)" ]
}

@test "upgrade builds the archive" {
  run "$INSTALL/starlake.sh" _do_upgrade --version 9.9.9
  [ "$status" -eq 0 ]
  [ -s "$(archive)" ]
}

@test "a training failure does not fail the install" {
  STUB_SUBMIT_EXIT=1 run "$INSTALL/starlake.sh" install
  [ "$status" -eq 0 ]
  [ ! -e "$(archive)" ]
  [[ "$output" == *"Installation done"* ]]
}

@test "a dry run install builds nothing" {
  run "$INSTALL/starlake.sh" install --dry-run
  [ "$status" -eq 0 ]
  [ ! -e "$(archive)" ]
  [ -z "$(last_submit)" ]
}

@test "install with SL_APPCDS=false builds nothing" {
  SL_APPCDS=false run "$INSTALL/starlake.sh" install
  [ "$status" -eq 0 ]
  [ ! -e "$(archive)" ]
}

@test "a jar added to a symlinked bin/deps makes the archive stale" {
  mv "$INSTALL/bin/deps" "$BATS_TEST_TMPDIR/real-deps"
  ln -s "$BATS_TEST_TMPDIR/real-deps" "$INSTALL/bin/deps"
  "$INSTALL/starlake.sh" appcds-build
  touch "$BATS_TEST_TMPDIR/real-deps/added.jar"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

# Background rebuild

@test "a stale archive is rebuilt in the background and the next command uses it" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
  wait_for_rebuild 2
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_active
  [ "$(training_count)" -eq 2 ]
}

@test "the command does not wait for the background rebuild" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  # Training stays blocked on the gate: waiting for it would exhaust the 20s stub loop.
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate"
  start=$SECONDS
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [ $((SECONDS - start)) -lt 10 ]
  wait_for_training_count 2
  pgrep -f "$INSTALL/starlake.sh appcds-build" >/dev/null
}

@test "the background rebuild does not hold the output of the command open" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate"
  start=$SECONDS
  "$INSTALL/starlake.sh" validate 2>&1 | cat >/dev/null
  [ $((SECONDS - start)) -lt 10 ]
}

@test "commands during a rebuild start no other build and print no warning" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate"
  "$INSTALL/starlake.sh" validate
  wait_for_training_count 2
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
  [[ "$output" != *"appcds-build"* ]]
  [[ "$output" != *"rebuilding"* ]]
  assert_no_new_training 2
}

@test "the old archive stays in place during a rebuild" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate"
  "$INSTALL/starlake.sh" validate
  wait_for_training_count 2
  [ -s "$(archive)" ]
}

@test "a failed background rebuild is not retried and the warning names the fix" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_NO_DUMP=1
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  wait_for_rebuild 2
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
  [[ "$output" == *"appcds-build"* ]]
  assert_no_new_training 2
}

@test "a failed rebuild is retried once the jars change again" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  STUB_NO_DUMP=1 "$INSTALL/starlake.sh" validate
  wait_for_rebuild 2
  touch "$INSTALL/bin/deps/added2.jar"
  run "$INSTALL/starlake.sh" validate
  wait_for_rebuild 3
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
}

@test "a manual build clears a recorded failure" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  STUB_NO_DUMP=1 "$INSTALL/starlake.sh" validate
  wait_for_rebuild 2
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
  [[ "$output" != *"appcds-build"* ]]
}

@test "a read-only install gets the warning and no build" {
  [ "$(id -u)" -ne 0 ] || skip "root ignores directory permissions"
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  chmod a-w "$INSTALL/.appcds"
  run "$INSTALL/starlake.sh" validate
  status_seen=$status
  chmod u+w "$INSTALL/.appcds"
  [ "$status_seen" -eq 0 ]
  assert_cds_inactive
  [[ "$output" == *"appcds-build"* ]]
  assert_no_new_training 1
}

@test "the lock of a build killed long ago is reclaimed" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  mkdir "$INSTALL/.appcds/build.lock"
  touch -t 202001010000 "$INSTALL/.appcds/build.lock"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  wait_for_rebuild 2
  [ ! -e "$INSTALL/.appcds/build.lock" ]
}

@test "a recent lock is respected" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  mkdir "$INSTALL/.appcds/build.lock"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_no_new_training 1
}

@test "no archive never starts a build" {
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_no_new_training 0
}

@test "commands outside the AppCDS conditions never start a build" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  SL_APPCDS=false "$INSTALL/starlake.sh" validate
  SPARK_MASTER_URL=spark://cluster:7077 "$INSTALL/starlake.sh" validate
  SPARK_CONF_DIR=/my/conf "$INSTALL/starlake.sh" validate
  SPARK_DRIVER_OPTIONS="-XX:SharedArchiveFile=/mine.jsa" "$INSTALL/starlake.sh" validate
  SL_RUN_MODE=main "$INSTALL/starlake.sh" validate
  assert_no_new_training 1
}

@test "spark config beyond logging never starts a build" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar" "$INSTALL/bin/spark/conf/spark-defaults.conf"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_no_new_training 1
}

@test "a failed rebuild is retried when a jar is replaced under the same name" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  STUB_NO_DUMP=1 "$INSTALL/starlake.sh" validate
  wait_for_rebuild 2
  sleep 0.1
  touch "$INSTALL/bin/deps/dep.jar"
  run "$INSTALL/starlake.sh" validate
  wait_for_rebuild 3
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
}

@test "a jar added during a rebuild makes the new archive stale" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate"
  "$INSTALL/starlake.sh" validate
  wait_for_training_count 2
  touch "$INSTALL/bin/deps/added2.jar"
  touch "$STUB_TRAINING_GATE"
  wait_for_background_builds
  run "$INSTALL/starlake.sh" validate
  assert_cds_inactive
  wait_for_rebuild 3
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
}

@test "a jar replaced during a rebuild makes the new archive stale" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate"
  "$INSTALL/starlake.sh" validate
  wait_for_training_count 2
  touch "$INSTALL/bin/deps/dep.jar"
  touch "$STUB_TRAINING_GATE"
  wait_for_background_builds
  run "$INSTALL/starlake.sh" validate
  assert_cds_inactive
  wait_for_rebuild 3
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
}

@test "a failed rebuild is retried when a jar changed during it" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate" STUB_NO_DUMP=1
  "$INSTALL/starlake.sh" validate
  wait_for_training_count 2
  touch "$INSTALL/bin/deps/dep.jar"
  touch "$STUB_TRAINING_GATE"
  wait_for_background_builds
  run "$INSTALL/starlake.sh" validate
  wait_for_training_count 3
}

@test "an install moved to a path with a space warns and starts no build" {
  "$INSTALL/starlake.sh" appcds-build
  SPACED="$BATS_TEST_TMPDIR/my install"
  mv "$INSTALL" "$SPACED"
  JAVA_HOME="$SPACED/jdk" run "$SPACED/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$output" == *"appcds-build"* ]]
  [[ "$output" != *"rebuilding"* ]]
}

@test "the rebuild starts when the launcher runs through bash with a relative path" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  cd "$INSTALL"
  run bash starlake.sh validate
  [ "$status" -eq 0 ]
  wait_for_training_count 2
  wait_for_lock_release
  run bash starlake.sh validate
  assert_cds_active
  ! grep -q '^PATH_LAUNCHER' "$STUB_LOG"
}

@test "install through bash with a relative path builds this install, not one on the PATH" {
  cd "$INSTALL"
  run bash starlake.sh install
  [ "$status" -eq 0 ]
  [ -s "$(archive)" ]
  ! grep -q '^PATH_LAUNCHER' "$STUB_LOG"
}

@test "the rebuild survives the terminal closing" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  export STUB_TRAINING_GATE="$BATS_TEST_TMPDIR/gate"
  "$INSTALL/starlake.sh" validate
  wait_for_training_count 2
  pkill -HUP -f "$INSTALL/starlake.sh appcds-build"
  sleep 0.2
  touch "$STUB_TRAINING_GATE"
  wait_for_background_builds
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
}

@test "a manual build reclaims the lock of a build killed long ago" {
  "$INSTALL/starlake.sh" appcds-build
  mkdir "$INSTALL/.appcds/build.lock"
  touch -t 202001010000 "$INSTALL/.appcds/build.lock"
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ "$(training_count)" -eq 2 ]
  [ ! -e "$INSTALL/.appcds/build.lock" ]
}

@test "a successful rebuild forgets an earlier failure" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  STUB_NO_DUMP=1 "$INSTALL/starlake.sh" validate
  wait_for_rebuild 2
  touch "$INSTALL/bin/deps/added2.jar"
  "$INSTALL/starlake.sh" validate
  wait_for_rebuild 3
  # Back on the jars that failed: only a forgotten failure allows a new try.
  rm "$INSTALL/bin/deps/added2.jar"
  run "$INSTALL/starlake.sh" validate
  wait_for_rebuild 4
}

@test "the background rebuild runs at low priority" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  "$INSTALL/starlake.sh" validate
  wait_for_rebuild 2
  expected=$(( $(nice) + 10 ))
  [ "$expected" -le 19 ] || expected=19
  [[ "$(last_training)" == *"nice=$expected"* ]]
}

# C1 compiler for transforms on JDBC

C1_FLAG="-XX:TieredStopAtLevel=1"

# The launcher reads the project from the working directory, as SL_ROOT defaults to it.
write_project() {
  mkdir -p metadata/transform/dom
  cat >metadata/application.sl.yml <<'YML'
---
version: 1
application:
  connectionRef: "{{connectionRef}}"
  audit:
    sink:
      connectionRef: "{{connectionRef}}"
  connections:
    sparkLocal:
      type: "fs"
    duckdb:
      type: "jdbc"
      options:
        url: "jdbc:duckdb:{{SL_ROOT}}/datasets/duckdb.db"
    bigquery:
      type: "bigquery"
YML
  printf 'version: 1\nenv:\n  connectionRef: "duckdb"\n' >metadata/env.sl.yml
  printf 'version: 1\ntask:\n  name: "t"\n  domain: "dom"\n' >metadata/transform/dom/t.sl.yml
}

transform() { run "$INSTALL/starlake.sh" transform --name dom.t; [ "$status" -eq 0 ]; }
assert_c1() { [[ "$(last_submit)" == *"$C1_FLAG"* ]]; }
assert_no_c1() { [[ "$(last_submit)" != *"TieredStopAtLevel"* ]]; }

@test "a transform on a jdbc connection uses C1" {
  write_project
  transform
  assert_c1
}

@test "a transform on an fs connection does not use C1" {
  write_project
  printf 'env:\n  connectionRef: sparkLocal\n' >metadata/env.sl.yml
  transform
  assert_no_c1
}

@test "a transform on bigquery does not use C1" {
  write_project
  printf 'env:\n  connectionRef: bigquery\n' >metadata/env.sl.yml
  transform
  assert_no_c1
}

@test "the env file of SL_ENV overrides the default env file" {
  write_project
  printf 'env:\n  connectionRef: sparkLocal\n' >metadata/env.SPARK.sl.yml
  printf 'env:\n  connectionRef: duckdb\n' >metadata/env.sl.yml
  SL_ENV=SPARK run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
  printf 'env:\n  connectionRef: sparkLocal\n' >metadata/env.sl.yml
  printf 'env:\n  connectionRef: duckdb\n' >metadata/env.DUCKDB.sl.yml
  SL_ENV=DUCKDB run "$INSTALL/starlake.sh" transform --name dom.t
  assert_c1
}

@test "SL_ENV defined in the default env file selects the env file" {
  write_project
  printf 'env:\n  SL_ENV: DUCKDB\n  connectionRef: sparkLocal\n' >metadata/env.sl.yml
  printf 'env:\n  connectionRef: duckdb\n' >metadata/env.DUCKDB.sl.yml
  transform
  assert_c1
}

@test "a literal connectionRef without env files works" {
  write_project
  rm metadata/env.sl.yml
  sed -i.bak 's/connectionRef: "{{connectionRef}}"/connectionRef: duckdb/' metadata/application.sl.yml
  transform
  assert_c1
}

@test "transformConnectionRef wins over connectionRef" {
  write_project
  sed -i.bak 's/^  audit:/  transformConnectionRef: sparkLocal\n  audit:/' metadata/application.sl.yml
  transform
  assert_no_c1
  printf 'env:\n  connectionRef: sparkLocal\n' >metadata/env.sl.yml
  sed -i.bak 's/transformConnectionRef: sparkLocal/transformConnectionRef: duckdb/' metadata/application.sl.yml
  transform
  assert_c1
}

@test "a connectionRef nested before the top level one is not read" {
  write_project
  cat >metadata/application.sl.yml <<'YML'
application:
  audit:
    sink:
      connectionRef: duckdb
  connectionRef: sparkLocal
  connections:
    sparkLocal:
      type: fs
    duckdb:
      type: jdbc
YML
  transform
  assert_no_c1
}

@test "only the type of the selected connection counts" {
  write_project
  cat >metadata/application.sl.yml <<'YML'
application:
  connectionRef: first
  connections:
    first:
      options:
        type: jdbc
    second:
      type: jdbc
YML
  transform
  assert_no_c1
}

@test "a jdbc connection read through Spark does not use C1" {
  write_project
  sed -i.bak 's/type: "jdbc"/type: "jdbc"\n      sparkFormat: "jdbc"/' metadata/application.sl.yml
  transform
  assert_no_c1
}

@test "a connectionRef in the transforms may send a task elsewhere and disables C1" {
  write_project
  printf 'task:\n  connectionRef: sparkLocal\n' >metadata/transform/dom/other.sl.yml
  transform
  assert_no_c1
}

@test "an unresolved connection disables C1" {
  write_project
  rm metadata/env.sl.yml
  transform
  assert_no_c1
  printf 'env:\n  connectionRef: nowhere\n' >metadata/env.sl.yml
  transform
  assert_no_c1
}

@test "a project that cannot be read leaves the command working without C1" {
  transform
  assert_no_c1
  mkdir -p metadata
  printf ': : not yaml\n\t{{{\n' >metadata/application.sl.yml
  transform
  assert_no_c1
}

@test "quotes, single quotes, comments, CRLF and case are understood" {
  write_project
  printf "application:\r\n  connectionRef: 'duck' # default\r\n  connections:\r\n    \"duck\":\r\n      type: JDBC\r\n" >metadata/application.sl.yml
  rm metadata/env.sl.yml
  transform
  assert_c1
}

@test "only transform uses C1" {
  write_project
  run "$INSTALL/starlake.sh" validate
  assert_no_c1
}

@test "environment overrides of the connection or the metadata disable C1" {
  write_project
  SL_CONNECTION_REF=sparkLocal run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
  SL_ACTIVE_CONNECTION_REF=sparkLocal run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
  SL_TRANSFORM_CONNECTION_REF=sparkLocal run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
  SL_METADATA=/elsewhere run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
  connectionRef=sparkLocal run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
}

@test "SL_JDBC_C1=false turns C1 off" {
  write_project
  SL_JDBC_C1=false run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
}

@test "user compiler flags are not overridden" {
  write_project
  SPARK_DRIVER_OPTIONS="-XX:-TieredCompilation" run "$INSTALL/starlake.sh" transform --name dom.t
  [[ "$(last_submit)" == *"-XX:-TieredCompilation"* ]]
  assert_no_c1
}

@test "a non-local master does not use C1" {
  write_project
  SPARK_MASTER_URL=spark://cluster:7077 run "$INSTALL/starlake.sh" transform --name dom.t
  assert_no_c1
}

@test "C1 and the AppCDS archive work together" {
  write_project
  "$INSTALL/starlake.sh" appcds-build
  transform
  assert_c1
  assert_cds_active
}

@test "SL_ROOT locates the project" {
  write_project
  mkdir "$BATS_TEST_TMPDIR/other"
  cd "$BATS_TEST_TMPDIR/other"
  SL_ROOT="$BATS_TEST_TMPDIR/cwd" run "$INSTALL/starlake.sh" transform --name dom.t
  assert_c1
}

@test "a key named like the connection nested in another connection is not read" {
  write_project
  rm metadata/env.sl.yml
  cat >metadata/application.sl.yml <<'YML'
application:
  connectionRef: target
  connections:
    other:
      options:
        target:
          type: jdbc
YML
  transform
  assert_no_c1
}

@test "a key named like the connection outside the connections is not read" {
  write_project
  rm metadata/env.sl.yml
  cat >metadata/application.sl.yml <<'YML'
application:
  connectionRef: target
  connections:
    other:
      type: fs
  dagRef:
    target:
      type: jdbc
YML
  transform
  assert_no_c1
}

@test "comments and blank lines inside a connection are skipped" {
  write_project
  rm metadata/env.sl.yml
  cat >metadata/application.sl.yml <<'YML'
application:
  connectionRef: duck
  connections:

# the local database
    duck:

# its type
      type: jdbc # local
YML
  transform
  assert_c1
}
