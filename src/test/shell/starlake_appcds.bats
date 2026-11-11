#!/usr/bin/env bats
#
# Runs the real distrib/starlake.sh in a throwaway install tree. Only the process
# boundaries are stubbed: spark-submit, java and curl.
#
# The last section runs a real JVM, because only a JVM can tell whether it accepts an archive.
#
# Run with: bats src/test/shell/starlake_appcds.bats

LAUNCHER="$BATS_TEST_DIRNAME/../../../distrib/starlake.sh"
REAL_JAVA_HOME="${JAVA_HOME:-}"

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
opts=""; master=""; version=""; args=("$@")
for ((i = 0; i < $#; i++)); do
  case "${args[$i]}" in
    --driver-java-options) opts="${args[$((i + 1))]}" ;;
    --master) master="${args[$((i + 1))]}" ;;
    --version) version=1 ;;
  esac
done
if [ -n "$version" ]; then
  printf 'PROBE\tconf_dir=%s\tcwd=%s\n' "${SPARK_CONF_DIR:-}" "$PWD" >>"$STUB_LOG"
else
  printf 'SUBMIT\topts=%s\tmaster=%s\tconf_dir=%s\tsl_env=%s\tsl_root=%s\tmemory=%s\tnice=%s\tconf_options=%s\thadoop=%s\n' \
    "$opts" "$master" "${SPARK_CONF_DIR:-}" "${SL_ENV:-}" "${SL_ROOT:-}" "${SPARK_DRIVER_MEMORY:-}" "$(ps -o ni= -p $$ | tr -d ' ')" "${SPARK_CONF_OPTIONS:-}" "${HADOOP_CONF_DIR:-}${YARN_CONF_DIR:-}${SPARK_DIST_CLASSPATH:-}" >>"$STUB_LOG"
fi
# As spark-class does, on stderr and before running anything.
if [ -n "${SPARK_PRINT_LAUNCH_COMMAND:-}" ] && [ -z "${STUB_NO_LAUNCH:-}" ]; then
  echo "Spark Command: $JAVA_HOME/bin/java -cp /stub/sl.jar:/stub/slf4j.jar:/stub/jars/* -Xmx${SPARK_DRIVER_MEMORY:-} $opts --add-opens=java.base/stub=ALL-UNNAMED -Djdk.stub=true org.apache.spark.deploy.SparkSubmit --master $master" >&2
fi
[ -n "$version" ] && exit 0
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
# The wildcard of the Spark command, expanded with the jar Spark also lists on its own.
case "$*" in
  *-XshowSettings:properties*)
    printf 'SETTINGS\t%s\n' "$*" >>"$STUB_LOG"
    [ -z "${STUB_NO_SETTINGS:-}" ] || exit 1
    cat >&2 <<'OUT'
Property settings:
    file.encoding = UTF-8
    java.class.path = /stub/sl.jar
        /stub/slf4j.jar
        /stub/jars/b.jar
        /stub/slf4j.jar
        /stub/jars/a.jar
    java.class.version = 65.0
    java.home = /stub
OUT
    exit 0
    ;;
esac
last="${!#}"
printf 'JAVA\tspark_home=%s\thostname=%s\tlast=%s\t%s\n' "${SPARK_HOME:-}" "${SPARK_LOCAL_HOSTNAME:-}" "$last" "$*" >>"$STUB_LOG"
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
    HADOOP_CONF_DIR YARN_CONF_DIR SPARK_DIST_CLASSPATH SL_APPCDS SL_ENV SL_ROOT SL_RUN_MODE SL_DRY_RUN SL_DEBUG SL_HTTP_PORT HTTP_PROXY HTTPS_PROXY
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
  [[ "$(last_submit)" != *"starlake.classpath"* ]]
}

# JVM flag merging

@test "proxy flags reach the driver when the user also sets SPARK_DRIVER_OPTIONS" {
  HTTP_PROXY=http://proxy.example:3128 SPARK_DRIVER_OPTIONS="-Dmine=1" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(last_submit)" == *"-Dhttp.proxyHost=proxy.example"* ]]
  [[ "$(last_submit)" == *"-Dhttp.proxyPort=3128"* ]]
  [[ "$(last_submit)" == *"-Dmine=1"* ]]
}

# The proxy password goes to the JVM and nowhere else. "|| false" because bash 3.2, on macOS,
# does not stop a test on a failing [[ ]] unless it is the last command.

@test "proxy password reaches the driver and is not printed" {
  HTTP_PROXY=http://me:s3cret@proxy.example:3128 HTTPS_PROXY=http://me:s3cret@proxy.example:3128 \
    run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$output" == *"Using HTTP_PROXY"* ]] || false
  [[ "$output" == *"Using HTTPS_PROXY"* ]] || false
  [[ "$output" != *"s3cret"* ]] || false
  [[ "$(last_submit)" == *"-Dhttp.proxyPassword=s3cret"* ]] || false
  [[ "$(last_submit)" == *"-Dhttps.proxyPassword=s3cret"* ]] || false
}

@test "proxy password is not printed when the proxy URL cannot be parsed" {
  HTTP_PROXY=http://me:s3cret@proxy.example:3128/path run "$INSTALL/starlake.sh" validate
  [[ "$output" == *"Could not parse"* ]] || false
  [[ "$output" != *"s3cret"* ]] || false
}

@test "proxy password is not printed by a download through the proxy" {
  https_proxy=http://me:s3cret@proxy.example:3128 SL_INSECURE=1 run "$INSTALL/starlake.sh" upgrade --version 9.9.9
  [[ "$output" == *"using proxy"* ]] || false
  [[ "$output" != *"s3cret"* ]] || false
}

@test "proxy password is not written to the files of a background archive build" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  HTTP_PROXY=http://me:s3cret@proxy.example:3128 run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  wait_for_rebuild 2
  [[ "$(last_training)" == *"-Dhttp.proxyPassword=s3cret"* ]] || false
  [ -s "$INSTALL/.appcds/build.log" ]
  ! grep -r s3cret "$INSTALL/.appcds"
}

@test "proxy password is not written to the log of a background archive build that fails" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  STUB_SUBMIT_EXIT=1 HTTP_PROXY=http://me:s3cret@proxy.example:3128 run "$INSTALL/starlake.sh" validate
  wait_for_background_builds
  [[ "$(last_training)" == *"-Dhttp.proxyPassword=s3cret"* ]] || false
  ! grep -r s3cret "$INSTALL/.appcds"
}

@test "proxy password is not printed when an archive training fails" {
  HTTP_PROXY=http://me:s3cret@proxy.example:3128 STUB_SUBMIT_EXIT=1 run "$INSTALL/starlake.sh" appcds-build
  [[ "$(last_training)" == *"-Dhttp.proxyPassword=s3cret"* ]] || false
  [[ "$output" != *"s3cret"* ]] || false
}

# setup.sh, with its own curl: it installs what curl prints.
stub_setup() {
  export HOME="$BATS_TEST_TMPDIR/home" SHELL=/bin/bash
  mkdir -p "$HOME" "$BATS_TEST_TMPDIR/setup-jdk/bin"
  : >"$HOME/.bashrc"
  cat >"$BATS_TEST_TMPDIR/setup-jdk/bin/java" <<EOF
#!/usr/bin/env bash
echo 'openjdk version "$1.0.1" 2025-01-01' >&2
EOF
  chmod +x "$BATS_TEST_TMPDIR/setup-jdk/bin/java"
  export JAVA_HOME="$BATS_TEST_TMPDIR/setup-jdk"
  cat >"$BATS_TEST_TMPDIR/path/curl" <<'EOF'
#!/usr/bin/env bash
out=""
while [ $# -gt 0 ]; do
  [ "$1" = "-o" ] && out="$2"
  shift
done
if [ -n "$out" ]; then echo not-an-archive >"$out"; else printf '#!/usr/bin/env bash\necho "stub starlake $*"\n'; fi
printf 200
EOF
}

@test "setup.sh installs a launcher without the proxy password through a proxy" {
  stub_setup 17
  target="$BATS_TEST_TMPDIR/target"
  https_proxy=http://me:s3cret@proxy.example:3128 SL_INSECURE=1 \
    run bash "$BATS_TEST_DIRNAME/../../../distrib/setup.sh" --target="$target" --version=9.9.9
  [ "$status" -eq 0 ]
  [[ "$output" == *"stub starlake install"* ]] || false
  [[ "$output" == *"using proxy"* ]] || false
  [[ "$output" != *"s3cret"* ]] || false
  [ "$(head -n 1 "$target/starlake")" = '#!/usr/bin/env bash' ]
  ! grep -r s3cret "$target" "$HOME"
}

@test "setup.sh does not print the proxy password when it downloads a JDK" {
  stub_setup 8
  https_proxy=http://me:s3cret@proxy.example:3128 SL_INSECURE=1 \
    run bash "$BATS_TEST_DIRNAME/../../../distrib/setup.sh" --target="$BATS_TEST_TMPDIR/target" --version=9.9.9
  [[ "$output" == *"Installing an embedded Temurin"* ]] || false
  [[ "$output" == *"using proxy"* ]] || false
  [[ "$output" != *"s3cret"* ]] || false
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
  [[ "$(last_training)" == *"conf_options="$'\t'* ]]
  [[ "$(last_training)" != *"-Dmine=1"* ]]
}

@test "training leaves nothing behind in the working directory or the temp project" {
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ -z "$(ls -A "$BATS_TEST_TMPDIR/cwd")" ]
  training_root="$(last_training | tr '\t' '\n' | sed -n 's/^sl_root=//p')"
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
  expected=$(( $(ps -o ni= -p $$) + 10 ))
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

@test "a symlinked jar whose target changes makes the archive stale" {
  touch "$BATS_TEST_TMPDIR/real.jar"
  ln -s "$BATS_TEST_TMPDIR/real.jar" "$INSTALL/bin/deps/linked.jar"
  "$INSTALL/starlake.sh" appcds-build
  sleep 0.1
  touch "$BATS_TEST_TMPDIR/real.jar"
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_inactive
}

@test "a project without files prints no error" {
  transform
  [[ "$output" != *"awk"* ]]
  [[ "$output" != *"No such file"* ]]
}

@test "a connectionRef nested after the top level one is not read" {
  write_project
  rm metadata/env.sl.yml
  cat >metadata/application.sl.yml <<'YML'
application:
  connectionRef: sparkLocal
  audit:
    sink:
      connectionRef: duckdb
  connections:
    sparkLocal:
      type: fs
    duckdb:
      type: jdbc
YML
  transform
  assert_no_c1
}

@test "CRLF line endings are understood without a trailing comment" {
  write_project
  rm metadata/env.sl.yml
  printf 'application:\r\n  connectionRef: duck\r\n  connections:\r\n    duck:\r\n      type: jdbc\r\n' >metadata/application.sl.yml
  transform
  assert_c1
}

# SL_RUN_MODE=main: the JVM of spark-submit without spark-submit

DEDUPLICATED_CLASSPATH="/stub/sl.jar:/stub/slf4j.jar:/stub/jars/b.jar:/stub/jars/a.jar"
last_java() { grep '^JAVA' "$STUB_LOG" | tail -1; }
main() { SL_RUN_MODE=main run "$INSTALL/starlake.sh" "$@"; [ "$status" -eq 0 ]; }
assert_main_archive() {
  [[ "$(last_java)" == *"-XX:SharedArchiveFile=$(archive)"* ]]
  [[ "$(last_java)" == *"-cp $DEDUPLICATED_CLASSPATH"* ]]
  [[ "$(last_java)" == *"--add-opens=java.base/stub=ALL-UNNAMED -Djdk.stub=true"* ]]
}
assert_main_plain() {
  [[ "$(last_java)" != *"SharedArchiveFile"* ]]
  [[ "$(last_java)" == *"sun.security.krb5"* ]]
}

@test "main runs the Starlake main class without spark-submit" {
  main validate
  [[ "$(last_java)" == *"ai.starlake.job.Main validate" ]]
  [ -z "$(last_submit)" ]
}

@test "main without an archive has the classpath order of spark-submit" {
  main validate
  sl="$INSTALL/bin/sl/starlake-core_2.13-9.9.9-assembly.jar"
  [[ "$(last_java)" == *"-cp $sl:$INSTALL/bin/deps/dep.jar:$INSTALL/bin/spark/conf:$INSTALL/bin/spark/jars/*"* ]]
  assert_main_plain
}

@test "main uses the archive with the JVM flags and classpath recorded from spark-submit" {
  "$INSTALL/starlake.sh" appcds-build
  main validate
  assert_main_archive
  [[ "$(last_java)" != *"sun.security.krb5"* ]]
}

@test "main does not inherit the flags of the training" {
  "$INSTALL/starlake.sh" appcds-build
  main validate
  [[ "$(last_java)" != *"ArchiveClassesAtExit"* ]]
  [[ "$(last_java)" != *"-Xmx"* ]]
  [[ "$(last_java)" != *"org.apache.spark.deploy.SparkSubmit"* ]]
}

@test "main sets SPARK_HOME and SPARK_LOCAL_HOSTNAME as the default mode does" {
  main validate
  [[ "$(last_java)" == *"spark_home=$INSTALL/bin/spark"* ]]
  [[ "$(last_java)" == *"hostname=127.0.0.1"* ]]
}

@test "main with a stale archive rebuilds it and uses it next time" {
  "$INSTALL/starlake.sh" appcds-build
  touch "$INSTALL/bin/deps/added.jar"
  main validate
  assert_main_plain
  wait_for_rebuild 2
  main validate
  assert_main_archive
}

@test "an archive built without a recorded launch command leaves main plain" {
  STUB_NO_LAUNCH=1 "$INSTALL/starlake.sh" appcds-build
  [ -s "$(archive)" ]
  main validate
  assert_main_plain
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
}

@test "a rebuild without a recorded launch command does not keep the previous one" {
  "$INSTALL/starlake.sh" appcds-build
  STUB_NO_LAUNCH=1 "$INSTALL/starlake.sh" appcds-build
  main validate
  assert_main_plain
}

@test "a failed rebuild keeps main on the previous archive" {
  "$INSTALL/starlake.sh" appcds-build
  STUB_NO_DUMP=1 run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -ne 0 ]
  main validate
  assert_main_archive
}

@test "training ignores SL_RUN_MODE=main, as only spark-submit can record the command" {
  SL_RUN_MODE=main run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ -s "$(archive)" ]
  [ -n "$(last_training)" ]
}

@test "main applies SPARK_DRIVER_OPTIONS after its own flags" {
  SPARK_DRIVER_OPTIONS="-Dmine=1 -Dlog4j.configurationFile=/mine" main validate
  [[ "$(last_java)" == *"log4j2.properties"*"-Dmine=1 -Dlog4j.configurationFile=/mine"* ]]
}

@test "main sets the heap only when SPARK_DRIVER_MEMORY is set by the user" {
  main validate
  [[ "$(last_java)" != *"-Xmx"* ]]
  SPARK_DRIVER_MEMORY=2g main validate
  [[ "$(last_java)" == *"-Xmx2g"* ]]
}

@test "main appends the Hadoop configuration as Spark does" {
  "$INSTALL/starlake.sh" appcds-build
  HADOOP_CONF_DIR=/hadoop YARN_CONF_DIR=/yarn SPARK_DIST_CLASSPATH=/dist.jar main validate
  [[ "$(last_java)" == *"-cp $DEDUPLICATED_CLASSPATH:/hadoop:/yarn:/dist.jar "* ]]
  assert_main_archive
}

@test "main puts a custom SPARK_CONF_DIR on the classpath and does not use the archive" {
  "$INSTALL/starlake.sh" appcds-build
  SPARK_CONF_DIR=/my/conf main validate
  [[ "$(last_java)" == *":/my/conf:$INSTALL/bin/spark/jars/*"* ]]
  assert_main_plain
}

@test "main uses C1 for a transform on JDBC and not for other commands" {
  write_project
  main transform --name dom.t
  [[ "$(last_java)" == *"$C1_FLAG"* ]]
  main validate
  [[ "$(last_java)" != *"TieredStopAtLevel"* ]]
}

@test "main on a non-local master uses neither the archive nor C1" {
  write_project
  "$INSTALL/starlake.sh" appcds-build
  SPARK_MASTER_URL=spark://cluster:7077 main transform --name dom.t
  assert_main_plain
  [[ "$(last_java)" != *"TieredStopAtLevel"* ]]
}

@test "SL_APPCDS=false keeps main away from the archive" {
  "$INSTALL/starlake.sh" appcds-build
  SL_APPCDS=false main validate
  assert_main_plain
}

@test "import, xls2yml and yml2xls run through main with the archive" {
  "$INSTALL/starlake.sh" appcds-build
  for command in import xls2yml yml2xls; do
    : >"$STUB_LOG"
    run "$INSTALL/starlake.sh" "$command"
    [ "$status" -eq 0 ]
    [[ "$(last_java)" == *"ai.starlake.job.Main $command" ]]
    assert_main_archive
    [ -z "$(last_submit)" ]
  done
}

@test "main keeps arguments with spaces together" {
  main validate --name "a b"
  [[ "$(last_java)" == *$'last=a b\t'* ]]
}

# The classpath of the archive

argfile() { echo "$INSTALL/.appcds/starlake.classpath"; }
assert_own_classpath() { [[ "$(last_submit)" == *"@$(argfile) "* ]]; }
assert_spark_classpath() { [[ "$(last_submit)" != *"starlake.classpath"* ]]; }

@test "the build expands the classpath of Spark without duplicates and in the order of the JVM" {
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [ "$(cat "$(argfile)")" = "-cp $DEDUPLICATED_CLASSPATH" ]
}

@test "the expansion starts from the classpath Spark computes" {
  "$INSTALL/starlake.sh" appcds-build
  grep -q '^SETTINGS.*-cp /stub/sl.jar:/stub/slf4j.jar:/stub/jars/\*' "$STUB_LOG"
  grep -q "^PROBE.*conf_dir=$INSTALL/.appcds/conf" "$STUB_LOG"
}

@test "the training dumps the archive with the classpath of the commands" {
  "$INSTALL/starlake.sh" appcds-build
  [[ "$(last_training)" == *"@$(argfile).tmp "* ]]
}

@test "a command with a fresh archive uses the classpath of the archive" {
  "$INSTALL/starlake.sh" appcds-build
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_cds_active
  assert_own_classpath
}

@test "user driver options come after the classpath of the archive so they win" {
  "$INSTALL/starlake.sh" appcds-build
  SPARK_DRIVER_OPTIONS="-Dmine=1" run "$INSTALL/starlake.sh" validate
  [[ "$(last_submit)" == *"@$(argfile) "*"-Dmine=1"* ]]
}

@test "without an archive the classpath stays the one of Spark" {
  run "$INSTALL/starlake.sh" validate
  assert_spark_classpath
}

@test "classpath entries of the environment keep the classpath of Spark, which adds them" {
  "$INSTALL/starlake.sh" appcds-build
  for variable in HADOOP_CONF_DIR YARN_CONF_DIR SPARK_DIST_CLASSPATH; do
    run env "$variable=/added" "$INSTALL/starlake.sh" validate
    [ "$status" -eq 0 ]
    assert_cds_active
    assert_spark_classpath
  done
}

@test "a driver classpath in the Spark options keeps the classpath of Spark, which adds it" {
  "$INSTALL/starlake.sh" appcds-build
  for options in "--driver-class-path /added.jar" "--conf spark.driver.extraClassPath=/added.jar"; do
    SPARK_CONF_OPTIONS="$options" run "$INSTALL/starlake.sh" validate
    [ "$status" -eq 0 ]
    assert_cds_active
    assert_spark_classpath
  done
}

@test "training ignores the classpath entries of the caller environment" {
  HADOOP_CONF_DIR=/hadoop YARN_CONF_DIR=/yarn SPARK_DIST_CLASSPATH=/dist.jar run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  [[ "$(last_training)" == *"hadoop="* ]]
  [[ "$(last_training)" != *"/hadoop"* ]]
  [[ "$(last_training)" != *"/dist.jar"* ]]
}

@test "the expansion leaves nothing behind in the working directory" {
  run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -eq 0 ]
  ! grep "^PROBE.*cwd=$BATS_TEST_TMPDIR/cwd" "$STUB_LOG"
}

@test "an archive built without a Spark command is used with the classpath of Spark" {
  STUB_NO_LAUNCH=1 "$INSTALL/starlake.sh" appcds-build
  [ -s "$(archive)" ]
  [ ! -e "$(argfile)" ]
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
  assert_spark_classpath
}

@test "an archive built when the JVM cannot expand the classpath is used with the classpath of Spark" {
  STUB_NO_SETTINGS=1 "$INSTALL/starlake.sh" appcds-build
  [ -s "$(archive)" ]
  [ ! -e "$(argfile)" ]
  [[ "$(last_training)" != *"starlake.classpath"* ]]
  run "$INSTALL/starlake.sh" validate
  assert_cds_active
  assert_spark_classpath
}

@test "a rebuild without an expanded classpath does not keep the previous one" {
  "$INSTALL/starlake.sh" appcds-build
  STUB_NO_SETTINGS=1 "$INSTALL/starlake.sh" appcds-build
  [ ! -e "$(argfile)" ]
}

@test "a failed rebuild keeps the classpath of the previous archive" {
  "$INSTALL/starlake.sh" appcds-build
  STUB_NO_DUMP=1 run "$INSTALL/starlake.sh" appcds-build
  [ "$status" -ne 0 ]
  [ "$(cat "$(argfile)")" = "-cp $DEDUPLICATED_CLASSPATH" ]
  [ ! -e "$(argfile).tmp" ]
  run "$INSTALL/starlake.sh" validate
  assert_own_classpath
}

@test "main without an expanded classpath keeps the recorded one" {
  STUB_NO_SETTINGS=1 "$INSTALL/starlake.sh" appcds-build
  main validate
  [[ "$(last_java)" == *"-cp /stub/sl.jar:/stub/slf4j.jar:/stub/jars/* "* ]]
}

# JDKs that the bundled Hadoop cannot run on

jdk_warning="cannot run Starflow"
set_java_version() { echo "JAVA_VERSION=\"$1\"" >"$INSTALL/jdk/release"; }
# The launcher looks for the jar of SL_VERSION: give the stub jar the name of each version tried.
with_sl_version() {
  local jar="$INSTALL/bin/sl/starlake-core_2.13-$1-assembly.jar"
  [ -e "$jar" ] || ln -s "$INSTALL/bin/sl/starlake-core_2.13-9.9.9-assembly.jar" "$jar"
}

@test "up to 1.8, JDK 24 and newer print a warning that names the version, and the command still runs" {
  for version in 24.0.2 25.0.5 27-ea; do
    set_java_version "$version"
    : >"$STUB_LOG"
    with_sl_version 1.8.18
    SL_VERSION=1.8.18 run "$INSTALL/starlake.sh" validate
    [ "$status" -eq 0 ]
    [[ "$output" == *"$jdk_warning"* ]]
    [[ "$output" == *"Java ${version%%[.-]*} "* ]]
    [ -n "$(last_submit)" ]
  done
}

@test "up to 1.8, supported JDKs print no warning" {
  for version in 17.0.9 21.0.11 22.0.2 23.0.2; do
    set_java_version "$version"
    with_sl_version 1.8.18
    SL_VERSION=1.8.18 run "$INSTALL/starlake.sh" validate
    [ "$status" -eq 0 ]
    [[ "$output" != *"$jdk_warning"* ]]
  done
}

@test "from 1.9, JDKs older than 25 print a warning that names both versions, and the command still runs" {
  for sl in 1.9.0 1.9.1-SNAPSHOT 2.0.0; do
    for version in 1.8.0_402 17.0.9 21.0.11 24.0.2; do
      set_java_version "$version"
      : >"$STUB_LOG"
      with_sl_version "$sl"
      SL_VERSION="$sl" run "$INSTALL/starlake.sh" validate
      [ "$status" -eq 0 ]
      [[ "$output" == *"$jdk_warning $sl, which requires Java 25"* ]]
      [ -n "$(last_submit)" ]
    done
  done
}

@test "from 1.9, JDK 25 and newer print no warning" {
  for version in 25.0.1 26.0.2 27-ea; do
    set_java_version "$version"
    with_sl_version 1.9.0
    SL_VERSION=1.9.0 run "$INSTALL/starlake.sh" validate
    [ "$status" -eq 0 ]
    [[ "$output" != *"$jdk_warning"* ]]
  done
}

@test "the JDK warning goes to stderr so that it does not pollute the output of a command" {
  with_sl_version 1.9.0
  with_sl_version 1.8.18
  set_java_version 21.0.11
  [ -z "$(SL_VERSION=1.9.0 "$INSTALL/starlake.sh" validate 2>/dev/null)" ]
  set_java_version 25.0.5
  [ -z "$(SL_VERSION=1.8.18 "$INSTALL/starlake.sh" validate 2>/dev/null)" ]
}

@test "a JDK without a version prints no warning" {
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$output" != *"$jdk_warning"* ]]
}

@test "the JDK warning also applies to SL_RUN_MODE=main" {
  set_java_version 21.0.11
  SL_RUN_MODE=main run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$output" == *"$jdk_warning"* ]]
}

@test "the JDK warning finds the java on the PATH when JAVA_HOME is not set" {
  set_java_version 21.0.11
  mv "$INSTALL/jdk" "$BATS_TEST_TMPDIR/path-jdk"
  PATH="$BATS_TEST_TMPDIR/path-jdk/bin:$PATH" JAVA_HOME="" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$output" == *"$jdk_warning"* ]]
}

@test "commands that only manage the install print no JDK warning" {
  set_java_version 21.0.11
  run "$INSTALL/starlake.sh" --version
  [ "$status" -eq 0 ]
  [[ "$output" != *"$jdk_warning"* ]]
}

# A real JVM

# Jars with real classes and a spark-submit that builds its command as spark-class does: the
# driver classpath, the conf dir, slf4j-api on its own, the wildcard, then the driver options.
make_real_install() {
  if [ ! -x "$REAL_JAVA_HOME/bin/javac" ]; then
    [ -z "${CI:-}" ] || { echo "these tests need a JDK in JAVA_HOME" >&2; return 1; }
    skip "needs a JDK in JAVA_HOME"
  fi
  export JAVA_HOME="$REAL_JAVA_HOME"
  export REAL_LOG="$BATS_TEST_TMPDIR/classes.log"
  local src="$BATS_TEST_TMPDIR/src" out="$BATS_TEST_TMPDIR/out" name
  mkdir -p "$src/ai/starlake/job" "$src/org/apache/spark/deploy" "$src/stub" "$out"
  cat >"$src/ai/starlake/job/Main.java" <<'EOF'
package ai.starlake.job;
public class Main {
  public static void main(String[] args) {
    new stub.Logger(); new stub.Dep(); new stub.SparkCore();
    System.out.println("classpath=" + System.getProperty("java.class.path"));
  }
}
EOF
  cat >"$src/org/apache/spark/deploy/SparkSubmit.java" <<'EOF'
package org.apache.spark.deploy;
public class SparkSubmit {
  public static void main(String[] args) { ai.starlake.job.Main.main(args); }
}
EOF
  for name in Logger Dep SparkCore X1 X2 X3 X4 X5 X6; do
    printf 'package stub; public class %s {}\n' "$name" >"$src/stub/$name.java"
  done
  "$JAVA_HOME/bin/javac" -d "$out" "$src"/stub/*.java "$src/ai/starlake/job/Main.java" "$src/org/apache/spark/deploy/SparkSubmit.java"
  rm -f "$INSTALL"/bin/spark/jars/*.jar "$INSTALL"/bin/deps/*.jar "$INSTALL"/bin/sl/*.jar
  real_jar() { "$JAVA_HOME/bin/jar" cf "$1" -C "$out" "$2"; }
  real_jar "$INSTALL/bin/sl/starlake-core_2.13-9.9.9-assembly.jar" ai/starlake/job/Main.class
  real_jar "$INSTALL/bin/deps/dep.jar" stub/Dep.class
  real_jar "$INSTALL/bin/spark/jars/spark-core_2.13-4.1.3.jar" stub/SparkCore.class
  real_jar "$INSTALL/bin/spark/jars/spark-launcher.jar" org/apache/spark/deploy/SparkSubmit.class
  real_jar "$INSTALL/bin/spark/jars/slf4j-api-2.0.17.jar" stub/Logger.class
  # Other jars after slf4j-api in the expansion, wherever the directory order puts it.
  for name in X1 X2 X3 X4 X5 X6; do
    real_jar "$INSTALL/bin/spark/jars/$name.jar" "stub/$name.class"
  done
  cat >"$INSTALL/bin/spark/bin/spark-submit" <<'EOF'
#!/usr/bin/env bash
opts=""; driver_cp=""; version=""
while [ $# -gt 0 ]; do
  case "$1" in
    --driver-java-options) opts="$2"; shift 2 ;;
    --driver-class-path) driver_cp="$2"; shift 2 ;;
    --class|--master) shift 2 ;;
    --version) version=1; shift ;;
    *) break ;;
  esac
done
cp="$driver_cp:${SPARK_CONF_DIR:-$SPARK_HOME/conf}/:$SPARK_HOME/jars/slf4j-api-2.0.17.jar:$SPARK_HOME/jars/*"
cp="$cp${HADOOP_CONF_DIR:+:$HADOOP_CONF_DIR}"
[ -z "${SPARK_PRINT_LAUNCH_COMMAND:-}" ] || \
  echo "Spark Command: $JAVA_HOME/bin/java -cp $cp $opts org.apache.spark.deploy.SparkSubmit" >&2
[ -z "$version" ] || exit 0
set -f
exec "$JAVA_HOME/bin/java" -cp "$cp" $opts org.apache.spark.deploy.SparkSubmit
EOF
  "$INSTALL/starlake.sh" appcds-build
  [ -s "$(archive)" ]
}

assert_loaded_from_archive() {
  local class
  for class in ai.starlake.job.Main stub.Dep stub.SparkCore stub.Logger; do
    grep -q "$class source: shared objects file (top)" "$REAL_LOG"
  done
}

real_classpath() { printf '%s\n' "$1" | sed -n 's/^classpath=//p' | tail -1; }

@test "real JVM: spark-submit loads the classes from the archive" {
  make_real_install
  SPARK_DRIVER_OPTIONS="-Xlog:class+load:file=$REAL_LOG" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_loaded_from_archive
}

@test "real JVM: SL_RUN_MODE=main loads the classes from the archive" {
  make_real_install
  SL_RUN_MODE=main SPARK_DRIVER_OPTIONS="-Xlog:class+load:file=$REAL_LOG" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  assert_loaded_from_archive
}

@test "real JVM: SL_RUN_MODE=main with Hadoop configuration still loads the classes from the archive" {
  make_real_install
  mkdir "$BATS_TEST_TMPDIR/hadoop"
  HADOOP_CONF_DIR="$BATS_TEST_TMPDIR/hadoop" SL_RUN_MODE=main SPARK_DRIVER_OPTIONS="-Xlog:class+load:file=$REAL_LOG" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(real_classpath "$output")" == *":$BATS_TEST_TMPDIR/hadoop" ]]
  assert_loaded_from_archive
}

@test "real JVM: the classpath keeps the entries and the order of Spark, without duplicates" {
  make_real_install
  run env SL_APPCDS=false "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  plain="$(real_classpath "$output" | tr ':' '\n' | grep '\.jar$' | awk '!seen[$0]++')"
  [ "$(real_classpath "$output" | tr ':' '\n' | grep -c 'slf4j-api')" -eq 2 ]
  run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [ "$(real_classpath "$output" | tr ':' '\n' | grep '\.jar$')" = "$plain" ]
}

@test "real JVM: Hadoop configuration stays on the classpath of a spark-submit" {
  make_real_install
  mkdir "$BATS_TEST_TMPDIR/hadoop"
  HADOOP_CONF_DIR="$BATS_TEST_TMPDIR/hadoop" run "$INSTALL/starlake.sh" validate
  [ "$status" -eq 0 ]
  [[ "$(real_classpath "$output")" == *":$BATS_TEST_TMPDIR/hadoop" ]]
}
