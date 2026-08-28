#!/usr/bin/env bash

set -e

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"

export SL_SCRIPT_DIR="$SCRIPT_DIR"
export SL_API_STARLAKE_CORE_PATH="${SL_API_STARLAKE_CORE_PATH:-$SCRIPT_DIR/starlake}"

API_BIN_DIR="$SCRIPT_DIR/bin/api/bin"

SL_ROOT="${SL_ROOT:-`pwd`}"

# Prefer the embedded JDK installed by setup.sh when JAVA_HOME is not set,
# then put that java first on the SESSION PATH: some run paths invoke bare
# `java`, and the system PATH may pin an older default Java first.
if [ -z "${JAVA_HOME:-}" ] && [ -x "$SCRIPT_DIR/jdk/bin/java" ]
then
  export JAVA_HOME="$SCRIPT_DIR/jdk"
fi
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]
then
  export PATH="$JAVA_HOME/bin:$PATH"
fi

# ENABLE_* choices are recorded in versions.sh, but versions.sh is only
# `source`d (never `export`ed), so a selective install's choices would not
# reach the `java -cp setup.jar Setup` subprocess launch_setup spawns. Left
# unset, Setup.java's ENABLE_ALL defaults to true and every ENABLE_X is
# computed as `ENABLE_ALL || envIsTrueWithDefaultTrue(X)` - so exporting only
# the per-category flags is NOT enough, the ENABLE_ALL||... short-circuits
# them regardless; ENABLE_ALL itself must ALSO be forced to "false" for the
# per-category overrides below to have any effect at all. Used by both
# upgrade and reinstall, called BEFORE bin/deps is wiped/replaced and AFTER
# the old versions.sh has already been `source`d by the top-of-script case
# statement; a genuine fresh `install` never calls this, so its
# ENABLE_ALL-defaults-true / interactive selection behavior is unaffected.
#
# Two sources of truth, in priority order:
#   1. The OLD versions.sh's recorded ENABLE_X value, if that line existed -
#      explicit prior user intent, trusted verbatim over anything else.
#   2. Otherwise (the old versions.sh predates this category entirely - e.g.
#      an install from before ENABLE_FLIGHTSQL existed - see
#      inference-fix-report.md for the git-log evidence), jar presence in
#      bin/deps is only a valid signal for categories already known to exist
#      in every install this old ("legacy" list below). For a category NOT
#      on that list, its jar being absent just means it never had a chance
#      to be installed yet, not that the user opted out - so it must be left
#      completely UNSET, letting Setup.java's own default-true provision it,
#      exactly like a fresh install would.
_LEGACY_ENABLE_CATEGORIES=" ENABLE_BIGQUERY ENABLE_AZURE ENABLE_SNOWFLAKE ENABLE_REDSHIFT ENABLE_POSTGRESQL ENABLE_DUCKDB ENABLE_KAFKA ENABLE_MARIA ENABLE_TRINODB "
_infer_one_enable_flag() {
  local var_name="$1"; shift
  local old_var_name="OLD_${var_name}"
  local old_val="${!old_var_name:-}"
  if [ -n "$old_val" ]; then
    export "$var_name=$old_val"
    return
  fi
  case "$_LEGACY_ENABLE_CATEGORIES" in
    *" $var_name "*) ;;
    *) return ;;
  esac
  local pattern
  for pattern in "$@"; do
    if compgen -G "$SCRIPT_DIR/bin/deps/$pattern" > /dev/null 2>&1; then
      export "$var_name=true"
      return
    fi
  done
  export "$var_name=false"
}
infer_enable_flags_from_deps() {
  # Snapshot whatever the old versions.sh recorded (already `source`d into
  # plain, non-exported shell vars before this function runs) BEFORE the
  # exports below start overwriting those same variable names in place. An
  # unset OLD_ENABLE_X means the old versions.sh had no such line at all.
  local _v
  for _v in ENABLE_BIGQUERY ENABLE_AZURE ENABLE_SNOWFLAKE ENABLE_REDSHIFT ENABLE_POSTGRESQL ENABLE_TRINODB ENABLE_KAFKA ENABLE_DUCKDB ENABLE_FLIGHTSQL; do
    eval "OLD_$_v=\"\${$_v:-}\""
  done
  # ENABLE_MARIA is the odd one out: the *file key* versions.sh persists is
  # ENABLE_MARIADB (generateVersions()'s own naming, verified back to v1.6.0),
  # but the *runtime key* Setup.java reads via envIsTrueWithDefaultTrue is
  # ENABLE_MARIA - so the old recorded value must be read from ENABLE_MARIADB,
  # not ENABLE_MARIA (which was never set by sourcing the old file at all).
  OLD_ENABLE_MARIA="${ENABLE_MARIADB:-}"
  export ENABLE_ALL=false
  # NOTE: Setup.java's field is ENABLE_MARIADB but the env var it actually
  # reads is "ENABLE_MARIA" (envIsTrueWithDefaultTrue("ENABLE_MARIA")) - a
  # pre-existing field/env-var name mismatch in Setup.java itself. Every
  # other flag below has a matching field/env-var name (verified against
  # every `envIsTrueWithDefaultTrue("ENABLE_...")` call in Setup.java).
  _infer_one_enable_flag ENABLE_BIGQUERY "spark-*bigquery*.jar"
  _infer_one_enable_flag ENABLE_AZURE "hadoop-azure-*.jar"
  _infer_one_enable_flag ENABLE_SNOWFLAKE "snowflake-jdbc-*.jar" "spark-snowflake_*.jar"
  _infer_one_enable_flag ENABLE_REDSHIFT "redshift-jdbc42-*.jar" "spark-redshift_*.jar"
  _infer_one_enable_flag ENABLE_POSTGRESQL "postgresql-*.jar"
  _infer_one_enable_flag ENABLE_MARIA "mariadb-java-client-*.jar"
  _infer_one_enable_flag ENABLE_TRINODB "trino-jdbc-*.jar"
  _infer_one_enable_flag ENABLE_KAFKA "kafka-avro-serializer-*.jar" "kafka-schema-registry-client-*.jar"
  _infer_one_enable_flag ENABLE_DUCKDB "duckdb_jdbc-*.jar"
  _infer_one_enable_flag ENABLE_FLIGHTSQL "flight-sql-jdbc-driver-*.jar"
}

case "$1" in
  reinstall)
    # Preserve the currently-pinned SL_VERSION (if any) across the wipe below and
    # export it so the java Setup subprocess launched by launch_setup reinstalls
    # AT that version instead of falling back to "latest github release".
    if [ -f "$SCRIPT_DIR/versions.sh" ]
    then
      source "$SCRIPT_DIR/versions.sh"
    fi
    if [ -n "$SL_VERSION" ]; then
      export SL_VERSION
    fi
    # Capture which connectors were actually installed BEFORE bin/deps is
    # wiped below - reinstall is meant to heal a poisoned install back to a
    # consistent state at the same SL_VERSION, not silently switch a
    # selective install into an ENABLE_ALL one.
    infer_enable_flags_from_deps
    rm -f "$SCRIPT_DIR/versions.sh"
    rm -rf "$SCRIPT_DIR/bin/spark" "$SCRIPT_DIR/bin/deps" "$SCRIPT_DIR/bin/sl"
    ;;
  *)
    if [ -f "$SCRIPT_DIR/versions.sh" ]
    then
      source "$SCRIPT_DIR/versions.sh"
    fi
    ;;
esac

# Launch-time consistency guard: rescue installs left poisoned by an older
# starlake.sh whose `upgrade` only swapped the core jar/API and never touched
# bin/spark (versions.sh could end up claiming a SPARK_VERSION that the actual
# jars on disk do not match), or by a re-provision that was interrupted after
# wiping bin/spark but before the download completed (bin/spark absent
# entirely - just as inconsistent, and just as silent otherwise). Gate on
# versions.sh existing: a genuinely fresh, never-installed tree has no
# versions.sh yet, and bin/spark not existing there is normal, not an error.
# Skipped for the commands that are themselves how you fix this.
if [ -z "$SL_SKIP_CONSISTENCY_CHECK" ] && [ -f "$SCRIPT_DIR/versions.sh" ] && [ -n "$SPARK_VERSION" ]
then
  case "$1" in
    install|reinstall|upgrade|_do_upgrade) ;;
    *)
      if [ ! -d "$SCRIPT_DIR/bin/spark/jars" ] || ! compgen -G "$SCRIPT_DIR/bin/spark/jars/spark-core_*-${SPARK_VERSION}.jar" > /dev/null 2>&1
      then
        echo "ERROR: Starlake installation is inconsistent." >&2
        echo "versions.sh declares Spark $SPARK_VERSION but $SCRIPT_DIR/bin/spark/jars has no matching spark-core jar (or is missing entirely - a re-provision may have been interrupted)." >&2
        echo "This usually happens after upgrading with an older starlake.sh that did not refresh the Spark runtime, or an upgrade/reinstall that did not finish." >&2
        echo "Run '$SCRIPT_DIR/starlake.sh reinstall' to fix it (wipes and re-downloads bin/spark, bin/deps and bin/sl for the currently pinned SL_VERSION)." >&2
        echo "Set SL_SKIP_CONSISTENCY_CHECK=1 to bypass this check." >&2
        exit 1
      fi
      ;;
  esac
fi

# Same default as starlake.cmd; versions.sh normally provides it, but the
# spark-core jar globs below must also work before a first versions.sh exists.
SCALA_VERSION="${SCALA_VERSION:-2.13}"
SL_ARTIFACT_NAME=starlake-core_$SCALA_VERSION
SPARK_DIR_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARK_TARGET_FOLDER=$SCRIPT_DIR/bin/spark
SPARK_EXTRA_LIB_FOLDER=$SCRIPT_DIR/bin
DEPS_EXTRA_LIB_FOLDER=$SPARK_EXTRA_LIB_FOLDER/deps
STARLAKE_EXTRA_LIB_FOLDER=$SPARK_EXTRA_LIB_FOLDER/sl
SL_SQL_WH="${SL_DATASETS:-$SL_ROOT/datasets}"

export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"
export SL_MAIN=ai.starlake.job.Main
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-local[*]}"
# export SL_PYTHON_LIBS_DIR="${SL_PYTHON_LIBS_DIR:-$SCRIPT_DIR/bin/deps/python-libs}"
if [ -n "$SL_VERSION" ]
then
  SL_JAR_NAME=$SL_ARTIFACT_NAME-$SL_VERSION-assembly.jar
fi

if [[ -n "${https_proxy}" ]] || [[ -n "${http_proxy}" ]]; then
  PROXY=${https_proxy:-$http_proxy}
fi


parse_proxy_and_build_args() {
    local type=$1
    local url=$2

    # If no protocol (e.g., "myproxy.com:8080"), add a default "http://"
    # so the regex can parse it correctly.
    if ! [[ $url == *"://"* ]]; then
        echo "No protocol found in ${type^^}_PROXY. Assuming 'http://'." >&2
        url="http://$url"
    fi

    # Use a regex to parse the URL components.
    # This regex captures:
    # 1: Protocol (which we ignore, using $type)
    # 2: User-pass (optional)
    # 3: Username (if user-pass exists)
    # 4: Password (if user-pass exists)
    # 5: Host (required)
    # 6: Port (optional, with colon)
    # 7: Port number (if port exists)
    local regex="^([^:]+)://(([^:]+):([^@]+)@)?([^:/]+)(:([0-9]+))?/?$"

    if [[ $url =~ $regex ]]; then
        local host="${BASH_REMATCH[5]}"
        local port="${BASH_REMATCH[7]}"
        local user="${BASH_REMATCH[3]}"
        local pass="${BASH_REMATCH[4]}"

        local args=""

        # Set host
        if [ -n "$host" ]; then
            args="-D${type}.proxyHost=${host}"
        else
            # If we can't find a host, the URL is invalid.
            return
        fi

        # Set port
        if [ -n "$port" ]; then
            args="$args -D${type}.proxyPort=${port}"
        fi

        # Set username
        if [ -n "$user" ]; then
            # URL-decode the username (e.g., %40 -> @)
            user_decoded=$(printf '%b' "${user//%/\\x}")
            args="$args -D${type}.proxyUser=${user_decoded}"
        fi

        # Set password
        if [ -n "$pass" ]; then
            # URL-decode the password
            pass_decoded=$(printf '%b' "${pass//%/\\x}")
            args="$args -D${type}.proxyPassword=${pass_decoded}"
        fi

        echo "$args"
    else
        echo "Warning: Could not parse ${type} proxy URL: $url" >&2
    fi
}

# All Java arguments will be collected in this array
export JAVA_ARGS=()

# The JVM prefers IPv4 by default; on dual-stack networks where an IPv4 path is
# broken this makes it pick a dead address that curl/python would avoid.
# "system" follows the OS address ordering (RFC 6724) and behaves identically
# on IPv4-only networks. Users can still override via SPARK_DRIVER_OPTIONS/JAVA_OPTS.
case "$JAVA_OPTS $SPARK_DRIVER_OPTIONS" in
  *java.net.preferIPv6Addresses*) ;;
  *) JAVA_ARGS+=("-Djava.net.preferIPv6Addresses=system") ;;
esac

# 1. Check for HTTPS_PROXY
if [ -n "$HTTPS_PROXY" ]; then
    # Pass "https" as the type and the variable's value
    https_args=$(parse_proxy_and_build_args "https" "$HTTPS_PROXY")
    if [ -n "$https_args" ]; then
        echo "Using HTTPS_PROXY: $HTTPS_PROXY"
        # Add the arguments to our array
        # We don't quote $https_args so that bash splits it into separate arguments
        JAVA_ARGS+=($https_args)
    fi
fi

# 2. Check for HTTP_PROXY
if [ -n "$HTTP_PROXY" ]; then
    # Pass "http" as the type and the variable's value
    http_args=$(parse_proxy_and_build_args "http" "$HTTP_PROXY")
    if [ -n "$http_args" ]; then
        echo "Using HTTP_PROXY: $HTTP_PROXY"
        JAVA_ARGS+=($http_args)
    fi
fi

if [ -n "$SPARK_DRIVER_OPTIONS" ]; then
  SPARK_DRIVER_OPTIONS="$SPARK_DRIVER_OPTIONS "
else
  SPARK_DRIVER_OPTIONS="${JAVA_ARGS[@]}"
fi

export JAVA_OPTS="$JAVA_OPTS ${JAVA_ARGS[@]}"

get_binary_from_url() {
    local url=$1
    local target_file=$2
    if [ -n "$PROXY" ] && [ -n "$SL_INSECURE" ]; then
        echo "Downloading $url to $target_file using proxy $PROXY"
        local response=$(curl -L --insecure --proxy "$PROXY" --progress-bar -w "%{http_code}" -o "$target_file" "$url")
    else
        local response=$(curl -L --progress-bar -w "%{http_code}" -o "$target_file" "$url")
    fi
    local status_code=${response: -3}

    if [[ ! $status_code =~ ^(2|3)[0-9][0-9]$ ]]; then
        echo "Error: Failed to retrieve data from $url. HTTP status code: $status_code"
        if [[ "$status_code" == "403" ]]; then
            echo "Hint: the GitHub API rate limit may be exceeded (60 requests/hour per IP). Retry later." >&2
        fi
        exit 1
    fi
}

get_content_from_url() {
    local url=$1
    if [ -n "$PROXY" ] && [ -n "$SL_INSECURE" ]; then
        local response=$(curl -L --insecure --proxy "$PROXY" -s -w "%{http_code}" "$url")
    else
        local response=$(curl -L -s -w "%{http_code}" "$url")
    fi
    local status_code=${response: -3}

    if [[ ! $status_code =~ ^(2|3)[0-9][0-9]$ ]]; then
        echo "Error: Failed to retrieve data from $url. HTTP status code: $status_code" >&2
        if [[ "$status_code" == "403" ]]; then
            echo "Hint: the GitHub API rate limit may be exceeded (60 requests/hour per IP). Retry later." >&2
        fi
        exit 1
    fi

    # Print the content excluding the status code
    local content_length=${#response}
    local content="${response:0:content_length-3}"
    echo "$content"
}

menu_select() {
    local prompt="$1"
    shift
    local options=("$@")
    local cur=0
    local count=${#options[@]}
    local esc=$(printf "\033")

    # Hide cursor
    echo -en "\033[?25l" >&2

    echo "$prompt" >&2
    for ((i=0; i<count; i++)); do
        if [ $i -eq $cur ]; then
            echo -e " > \033[1m${options[$i]}\033[0m" >&2
        else
            echo "   ${options[$i]}" >&2
        fi
    done

    while true; do
        read -rsn1 key
        if [[ "$key" == "$esc" ]]; then
            read -rsn2 key
            if [[ "$key" == "[A" ]]; then
                cur=$((cur - 1))
                [ $cur -lt 0 ] && cur=$((count - 1))
            elif [[ "$key" == "[B" ]]; then
                cur=$((cur + 1))
                [ $cur -ge $count ] && cur=0
            fi
        elif [[ "$key" == "" ]]; then
            break
        fi

        # Move up count lines
        echo -en "\033[${count}A" >&2
        for ((i=0; i<count; i++)); do
            if [ $i -eq $cur ]; then
                echo -e " > \033[1m${options[$i]}\033[0m\033[K" >&2
            else
                echo -e "   ${options[$i]}\033[K" >&2
            fi
        done
    done

    # Show cursor
    echo -en "\033[?25h" >&2
    SELECTED_OPTION="${options[$cur]}"
}

select_starlake_version() {
    # Non-interactive override: `upgrade --version X.Y.Z` / SL_UPGRADE_VERSION,
    # so scripted/CI/Docker upgrades don't have to fake a keypress on the menu.
    local forced_version="$1"
    if [ -n "$forced_version" ]; then
        NEW_SL_VERSION="$forced_version"
        echo "Selected version: $NEW_SL_VERSION (forced)"
        return
    fi

    RELEASES_JSON=$(get_content_from_url "https://api.github.com/repos/starlake-ai/starlake/releases?per_page=15")
    ALL_RELEASE_VERSIONS=$(echo "$RELEASES_JSON" \
      | grep -o '"tag_name"[[:space:]]*:[[:space:]]*"v[0-9][^"]*"' \
      | sed -E 's/.*"v([^"]+)".*/\1/' \
      | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
      | sort -rV)

    if [ -z "$ALL_RELEASE_VERSIONS" ]; then
        echo "Error: no releases found at https://github.com/starlake-ai/starlake/releases" >&2
        exit 1
    fi

    LATEST_RELEASE_VERSIONS=$(echo "$ALL_RELEASE_VERSIONS" | head -n 5)
    VERSIONS=($LATEST_RELEASE_VERSIONS)

    menu_select "Select the version to install (use arrow keys):" "${VERSIONS[@]}"
    NEW_SL_VERSION="$SELECTED_OPTION"
    echo "Selected version: $NEW_SL_VERSION"
}

verify_sha256() {
    local file=$1
    local sha_url=$2
    if ! command -v shasum >/dev/null 2>&1; then
        echo "Warning: shasum not found, skipping checksum verification for $(basename "$file")"
        return 0
    fi
    get_binary_from_url "$sha_url" "$file.sha256"
    if ( cd "$(dirname "$file")" && shasum -a 256 -c "$(basename "$file").sha256" ); then
        rm -f "$file.sha256"
    else
        echo "Error: checksum verification failed for $file"
        exit 1
    fi
}

sync_spark_runtime() {
  # $1: git ref (release tag or "master") whose Setup.java pins the wanted
  # Spark version. Setup.java at that ref is the single source of truth for the
  # release's Spark/Hadoop/connector version pins - it is also what generates
  # versions.sh on a fresh install. Fetch just enough of it (its SPARK_VERSION
  # default) to decide whether the Spark runtime needs to be re-provisioned,
  # without duplicating those pins here. Setup.java itself only downloads Spark
  # when bin/spark is ABSENT, so wiping a mismatched runtime here is exactly
  # what makes the subsequent launch_setup re-provision it.
  # get_binary_from_url exits the whole script on a download failure (same
  # fail-fast behavior as every other download in this script), so no explicit
  # error handling is needed here.
  local target_ref="$1"
  local target_setup_java="$SCRIPT_DIR/.target-setup-java.tmp"
  get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starlake/$target_ref/src/main/java/Setup.java" "$target_setup_java"
  TARGET_SPARK_VERSION=$(grep -o 'getEnv("SPARK_VERSION")\.orElse("[^"]*")' "$target_setup_java" | head -n1 | sed -E 's/.*orElse\("([^"]*)"\).*/\1/')
  rm -f "$target_setup_java"

  if [ -z "$TARGET_SPARK_VERSION" ]; then
      echo "Warning: could not determine the target Spark version for $target_ref; re-provisioning bin/spark unconditionally to be safe." >&2
      rm -rf "$SCRIPT_DIR/bin/spark"
  elif ! compgen -G "$SCRIPT_DIR/bin/spark/jars/spark-core_${SCALA_VERSION}-${TARGET_SPARK_VERSION}.jar" > /dev/null 2>&1; then
      echo "Spark runtime is changing (${SPARK_VERSION:-none} -> $TARGET_SPARK_VERSION): re-provisioning bin/spark."
      rm -rf "$SCRIPT_DIR/bin/spark"
  else
      echo "Spark runtime already at $TARGET_SPARK_VERSION, keeping bin/spark as-is."
  fi
}

launch_setup() {
  # $1: optional git ref (tag, e.g. "v1.8.0") to fetch setup.jar from; defaults
  # to "master". Install, reinstall and upgrade all pass the target release tag
  # so Setup.java's compiled-in version defaults - its own generateVersions() is
  # what writes versions.sh - match that exact release rather than whatever
  # master happens to be at the time. Only versions with no release tag to fetch
  # from (SNAPSHOTs, local builds) fall back to master.
  local ref="${1:-master}"
  local setup_url="https://raw.githubusercontent.com/starlake-ai/starlake/$ref/distrib/setup.jar"
  get_binary_from_url "$setup_url" "$SCRIPT_DIR/setup.jar"

  if [ -n "${JAVA_HOME}" ]; then
    RUNNER="${JAVA_HOME}/bin/java"
  else
    if [ "$(command -v java)" ]; then
      RUNNER="java"
    else
      echo "JAVA_HOME is not set" >&2
      exit 1
    fi
  fi
  if ! $RUNNER -cp "$SCRIPT_DIR/setup.jar" Setup "$SCRIPT_DIR" unix; then
    echo "Error: setup failed - the installation is incomplete." >&2
    exit 1
  fi

  # if API_BIN_DIR exists set all files starting with local- as executable
  if [ -d "$API_BIN_DIR" ]; then
    for file in "$API_BIN_DIR"/local-*; do
      if [ -f "$file" ]; then
        chmod +x "$file"
      fi
    done
  fi
}

launch_starlake() {
  if [ -f "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" ]
  then
    if  [ -n "$SL_LOG_LEVEL" ] && [ "$SL_LOG_LEVEL" != "error" ]; then
      echo "- JAVA_HOME=$JAVA_HOME"
      echo "- SL_ROOT=$SL_ROOT"
    fi
    if [ "$SL_ENV" != "" ]; then
      echo "- SL_ENV=$SL_ENV"
    fi
#    echo "- SL_MAIN=$SL_MAIN"
#    echo "- SL_VALIDATE_ON_LOAD=$SL_VALIDATE_ON_LOAD"
#    echo "- SPARK_DRIVER_MEMORY=$SPARK_DRIVER_MEMORY"
#    echo Make sure your java home path does not contain space


    #if [[ $SL_FS = abfs:* ]] || [[ $SL_FS = wasb:* ]] || [[ $SL_FS = wasbs:* ]]
    #then
    #  if [[ -z "$AZURE_STORAGE_ACCOUNT" ]]
    #  then
    #    echo "AZURE_STORAGE_ACCOUNT should reference storage account name"
    #    exit 1
    #  fi
    #  if [[ -z "$AZURE_STORAGE_KEY" ]]
    #  then
    #    echo "AZURE_STORAGE_KEY should reference the storage account key"
    #    exit 1
    #  fi
    #  export SL_STORAGE_CONF="fs.azure.account.auth.type.$AZURE_STORAGE_ACCOUNT.blob.core.windows.net=SharedKey,
    #                  fs.azure.account.key.$AZURE_STORAGE_ACCOUNT.blob.core.windows.net="$AZURE_STORAGE_KEY",
    #                  fs.default.name=$SL_FS,
    #                  fs.defaultFS=$SL_FS"
    #fi

    if [[ -z "$SL_DEBUG" ]]
    then
      SPARK_DRIVER_OPTIONS="$SPARK_DRIVER_OPTIONS" # "-Dlog4j.configuration=$SPARK_TARGET_FOLDER/conf/log4j2.properties"
    else
      SPARK_DRIVER_OPTIONS="$SPARK_DRIVER_OPTIONS -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005" # -Dlog4j.configuration=$SPARK_TARGET_FOLDER/conf/log4j2.properties"
    fi

    if [[ "$1" =~ ^(import|xls2yml|yml2xls)$ ]]
    then
      SL_RUN_MODE=main
    fi

    if [[ "$SL_RUN_MODE" == "main" ]]
    then
      SL_ROOT=$SL_ROOT java \
                          --add-opens=java.base/java.lang=ALL-UNNAMED \
                          --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
                          --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
                          --add-opens=java.base/java.io=ALL-UNNAMED \
                          --add-opens=java.base/java.net=ALL-UNNAMED \
                          --add-opens=java.base/java.nio=ALL-UNNAMED \
                          --add-opens=java.base/java.util=ALL-UNNAMED \
                          --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
                          --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
                          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
                          --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
                          --add-opens=java.base/sun.security.action=ALL-UNNAMED \
                          --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
                          --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
                          -Dlog4j.configurationFile="$SPARK_TARGET_FOLDER/conf/log4j2.properties" \
                          -cp "$SPARK_TARGET_FOLDER/jars/*:$DEPS_EXTRA_LIB_FOLDER/*:$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" $SL_MAIN $@
    else
      extra_classpath="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME"
      extra_jars="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME"
      SPARK_SUBMIT="$SPARK_TARGET_FOLDER/bin/spark-submit"
      # the command below requires --jars "$extra_jars" to run on distributed modes
      if [[ $SPARK_MASTER_URL == local* ]]
      then
        if [ $(ls "$DEPS_EXTRA_LIB_FOLDER/"*.jar | wc -l) -ne 0 ]
        then
          extra_classpath="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME:$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ':')"
        fi
        SPARK_LOCAL_HOSTNAME="127.0.0.1" SPARK_HOME="$SCRIPT_DIR/bin/spark" SL_ROOT="$SL_ROOT" "$SPARK_SUBMIT" $SPARK_EXTRA_PACKAGES --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --driver-class-path "$extra_classpath" --class "$SL_MAIN" --master "$SPARK_MASTER_URL" "$SPARK_TARGET_FOLDER/README.md" "$@"
      else
        if [ $(ls "$DEPS_EXTRA_LIB_FOLDER/"*.jar | wc -l) -ne 0 ]
        then
          extra_classpath="$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ':')"
          extra_jars="$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ',')"

        fi
         SPARK_HOME="$SCRIPT_DIR/bin/spark" SL_ROOT="$SL_ROOT" "$SPARK_SUBMIT" $SPARK_EXTRA_PACKAGES $SPARK_CONF_OPTIONS --driver-java-options "$SPARK_DRIVER_OPTIONS" --driver-class-path "$extra_classpath" --class "$SL_MAIN" --master "$SPARK_MASTER_URL"  --jars $extra_jars "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" "$@"
      fi
    fi
  else
    echo "Starlake jar $SL_JAR_NAME does not exists. Please install it."
    exit 1
  fi
}


case "$1" in
  --version|version)
	  echo Starlake $SL_VERSION
	  echo Duckdb JDBC driver ${DUCKDB_VERSION}
	  echo BigQuery Spark connector ${SPARK_BQ_VERSION}
	  echo Hadoop for Azure ${HADOOP_AZURE_VERSION}
	  echo Azure Storage ${AZURE_STORAGE_VERSION}
	  echo Spark ${SPARK_VERSION}
	  echo Hadoop ${HADOOP_VERSION}
	  echo Snowflake Spark connector ${SPARK_SNOWFLAKE_VERSION}
	  echo Snowflake JDBC driver ${SNOWFLAKE_JDBC_VERSION}
	  echo Postgres JDBC driver ${POSTGRESQL_VERSION}
	  echo AWS SDK ${AWS_JAVA_SDK_V2_VERSION}
	  echo Hadoop for AWS ${HADOOP_AWS_VERSION}
	  echo Redshift JDBC driver ${REDSHIFT_JDBC_VERSION}
	  echo Redshift Spark connector ${SPARK_REDSHIFT_VERSION}
    ;;
  install|reinstall)
    # Pin setup.jar to the release being installed whenever SL_VERSION names
    # one. Setup.java's compiled-in defaults (Spark, Hadoop and every connector
    # pin) are what get written to versions.sh and provisioned into bin/, so
    # they have to come from the SAME release as the core jar. Taking them from
    # master instead can pair a core jar with a Spark it was never built against
    # - installing 1.7.x (built for Spark 3.5) while master pins Spark 4.
    # SL_VERSION here is either exported by the caller (setup.sh exports the
    # selected version before calling `install`) or read from versions.sh at the
    # top of this script (install/reinstall over an already-provisioned tree).
    # Non-release values - SNAPSHOTs, locally built versions - have no release
    # tag to fetch a setup.jar from, so they keep falling back to master.
    if [[ "$SL_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      TARGET_REF="v$SL_VERSION"
    else
      TARGET_REF="master"
    fi
    # A version-changing `install` over an existing tree must wipe a Spark
    # runtime that no longer matches the target release's pin (Setup.java only
    # downloads Spark when bin/spark is absent - see sync_spark_runtime).
    # reinstall already wiped bin/spark unconditionally at the top of this
    # script, and a fresh tree has no runtime to check, so both skip the probe.
    if [ "$1" = "install" ] && [ -d "$SCRIPT_DIR/bin/spark" ]; then
      sync_spark_runtime "$TARGET_REF"
    fi
    # Export so the java Setup subprocess installs THIS version. Without it, an
    # SL_VERSION read from versions.sh stays shell-local, Setup's getenv misses
    # it and falls back to "latest github release" - pairing a latest core jar
    # with the pinned release's setup.jar. reinstall exports at the top of this
    # script for the same reason.
    if [ -n "$SL_VERSION" ]; then
      export SL_VERSION
    fi
    launch_setup "$TARGET_REF"
    echo
    echo "Installation done. You're ready to enjoy Starlake!"
    echo If any errors happen during installation. Please try to install again or open an issue.
    ;;
  upgrade)
    # Self-update: download latest starlake.sh and re-launch, forwarding any
    # extra args (e.g. --version X.Y.Z) through to _do_upgrade.
    echo "Updating starlake script..."
    get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.sh" "$SCRIPT_DIR/starlake.sh.tmp"
    chmod +x "$SCRIPT_DIR/starlake.sh.tmp"
    mv "$SCRIPT_DIR/starlake.sh.tmp" "$SCRIPT_DIR/starlake.sh"
    shift
    exec "$SCRIPT_DIR/starlake.sh" _do_upgrade "$@"
    ;;
  _do_upgrade)
    shift
    # Non-interactive version selection: `upgrade --version X.Y.Z` (space or
    # --version=X.Y.Z form) or SL_UPGRADE_VERSION env var. Falls back to the
    # interactive arrow-key menu when neither is set (unchanged default).
    FORCED_SL_VERSION="${SL_UPGRADE_VERSION:-}"
    while [ $# -gt 0 ]; do
        case "$1" in
            --version=*) FORCED_SL_VERSION="${1#*=}" ;;
            --version) shift; FORCED_SL_VERSION="$1" ;;
        esac
        shift
    done
    select_starlake_version "$FORCED_SL_VERSION"
    if [ -n "$NEW_SL_VERSION" ]; then
        echo "Upgrading Starlake to $NEW_SL_VERSION..."

        TARGET_REF="v$NEW_SL_VERSION"

        # Wipe bin/spark if the target release pins a different Spark (see
        # sync_spark_runtime - shared with the `install` path).
        sync_spark_runtime "$TARGET_REF"

        # bin/deps is always refreshed by launch_setup below: Setup.java deletes
        # each dependency category by artefact-name match and re-downloads it at
        # the version pinned by the target release, so stale connector jars (e.g.
        # Delta/Iceberg/BigQuery/AWS SDK) get fixed even when Spark itself did not
        # change and even though their versions are never recorded in versions.sh.

        # See infer_enable_flags_from_deps (defined near the top of this
        # script) for why both ENABLE_ALL=false AND every per-category flag
        # must be exported explicitly for this to actually take effect.
        infer_enable_flags_from_deps

        export SL_VERSION="$NEW_SL_VERSION"

        # Re-provision via the real install machinery instead of a hand-rolled
        # download of the core jar/API zip: Setup.java (fetched pinned to
        # $TARGET_REF) replaces bin/sl, bin/api and bin/deps/python-libs, only
        # skips bin/spark when it is already present (handled above), and writes
        # a brand new versions.sh from scratch (no line-by-line sed patching).
        launch_setup "$TARGET_REF"

        echo "Upgrade complete."
    fi
    ;;
  serve)
    chmod +x $SCRIPT_DIR/bin/api/git/*.sh
    chmod +x $SCRIPT_DIR/bin/api/bin/*
    if [[ -z "$SL_API_DEBUG" ]]
    then
      export JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
                          --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
                          --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
                          --add-opens=java.base/java.io=ALL-UNNAMED \
                          --add-opens=java.base/java.net=ALL-UNNAMED \
                          --add-opens=java.base/java.nio=ALL-UNNAMED \
                          --add-opens=java.base/java.util=ALL-UNNAMED \
                          --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
                          --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
                          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
                          --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
                          --add-opens=java.base/sun.security.action=ALL-UNNAMED \
                          --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
                          --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED $JAVA_OPTS"
    else
      export JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
                          --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
                          --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
                          --add-opens=java.base/java.io=ALL-UNNAMED \
                          --add-opens=java.base/java.net=ALL-UNNAMED \
                          --add-opens=java.base/java.nio=ALL-UNNAMED \
                          --add-opens=java.base/java.util=ALL-UNNAMED \
                          --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
                          --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
                          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
                          --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
                          --add-opens=java.base/sun.security.action=ALL-UNNAMED \
                          --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
                          --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED $JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    fi
    $SCRIPT_DIR/bin/api/bin/local-run-api $SCRIPT_DIR dummy

    ;;
  *)
    if [[ -z "$SL_HTTP_PORT" ]]
    then
      launch_starlake "$@"
    else
      SL_HTTP_HOST=${SL_HTTP_HOST:-127.0.0.1}
      SL_SERVE_URI=http://$SL_HTTP_HOST:$SL_HTTP_PORT
      for value in validation run transform compile
      do
        log=$SL_ROOT/out/$value.log
        if [[ -f $log ]]
        then
          rm -f $log
        fi
      done
      curl  "$SL_SERVE_URI?ROOT=$SL_ROOT&PARAMS=$@"
      for value in validation run transform compile
      do
        log=$SL_ROOT/out/$value.log
        if [[ -f $log ]]
        then
          cat  $log
        fi
      done

    fi
    ;;
esac
