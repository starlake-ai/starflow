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

case "$1" in
  reinstall)
    # Recover SL_VERSION from the old versions.sh before removing it, so the
    # install case below can still pin setup.jar to that release. Only this one
    # value is read - the rest is deliberately left unset so Setup reprovisions
    # it from scratch, exactly as reinstall did before.
    if [ -z "${SL_VERSION:-}" ] && [ -f "$SCRIPT_DIR/versions.sh" ]
    then
      SL_VERSION="$(sed -n 's/^SL_VERSION=${SL_VERSION:-\(.*\)}$/\1/p' "$SCRIPT_DIR/versions.sh")"
      export SL_VERSION
    fi
    rm "$SCRIPT_DIR/versions.sh"
    rm -rf "$SCRIPT_DIR/bin/spark"
    ;;
  *)
    if [ -f "$SCRIPT_DIR/versions.sh" ]
    then
      source "$SCRIPT_DIR/versions.sh"
    fi
    ;;
esac

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

launch_setup() {
  # $1: optional git ref (tag, e.g. "v1.7.5") to fetch setup.jar from; defaults
  # to "master". Setup.java's compiled-in pins (Spark, Hadoop and every
  # connector version) are what get written to versions.sh and provisioned into
  # bin/, so they have to come from the SAME release as the core jar. Taking
  # them from master instead pairs a 1.7.x core jar (built for Spark 3.5) with
  # master's Spark 4 pins, which is how the 1.7 image build broke on
  # delta-spark_3.5_2.13/4.3.1 (no such artifact). Versions with no release tag
  # to fetch from - SNAPSHOTs, local builds - still fall back to master.
  local ref="${1:-master}"
  local setup_url=https://raw.githubusercontent.com/starlake-ai/starlake/$ref/distrib/setup.jar
  get_binary_from_url $setup_url "$SCRIPT_DIR/setup.jar"

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
  $RUNNER -cp "$SCRIPT_DIR/setup.jar" Setup "$SCRIPT_DIR" unix

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
	  echo AWS SDK ${AWS_JAVA_SDK_VERSION}
	  echo Hadoop for AWS ${HADOOP_AWS_VERSION}
	  echo Redshift JDBC driver ${REDSHIFT_JDBC_VERSION}
	  echo Redshift Spark connector ${SPARK_REDSHIFT_VERSION}
    ;;
  install|reinstall)
    # Pin setup.jar to the release being installed whenever SL_VERSION names
    # one. SL_VERSION is either exported by the caller (scripts/versions.sh
    # exports it from version.sbt for the image build) or read from versions.sh
    # at the top of this script. Non-release values - SNAPSHOTs, locally built
    # versions - have no release tag to fetch a setup.jar from.
    if [[ "${SL_VERSION:-}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      TARGET_REF="v$SL_VERSION"
    else
      TARGET_REF="master"
    fi
    # Export so the java Setup subprocess installs THIS version; without it an
    # SL_VERSION read from versions.sh stays shell-local and Setup falls back to
    # the latest github release, pairing a latest core jar with a pinned setup.
    if [ -n "${SL_VERSION:-}" ]; then
      export SL_VERSION
    fi
    launch_setup "$TARGET_REF"
    echo
    echo "Installation done. You're ready to enjoy Starlake!"
    echo If any errors happen during installation. Please try to install again or open an issue.
    ;;
  upgrade)
    # Self-update: download latest starlake.sh and re-launch
    echo "Updating starlake script..."
    get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.sh" "$SCRIPT_DIR/starlake.sh.tmp"
    chmod +x "$SCRIPT_DIR/starlake.sh.tmp"
    mv "$SCRIPT_DIR/starlake.sh.tmp" "$SCRIPT_DIR/starlake.sh"
    exec "$SCRIPT_DIR/starlake.sh" _do_upgrade
    ;;
  _do_upgrade)
    select_starlake_version
    if [ -n "$NEW_SL_VERSION" ]; then
        if [ -f "$SCRIPT_DIR/versions.sh" ]; then
             sed -i.bak "s/SL_VERSION=\${SL_VERSION:-.*}/SL_VERSION=\${SL_VERSION:-$NEW_SL_VERSION}/" "$SCRIPT_DIR/versions.sh"
             rm "$SCRIPT_DIR/versions.sh.bak"
             echo "Updated versions.sh with SL_VERSION=$NEW_SL_VERSION"
        fi
        export SL_VERSION=$NEW_SL_VERSION

        echo "Upgrading Starlake to $NEW_SL_VERSION..."

        BASE_URL="https://github.com/starlake-ai/starlake/releases/download/v${NEW_SL_VERSION}"

        SL_LIB_DIR="$STARLAKE_EXTRA_LIB_FOLDER"
        BIN_DIR="$SCRIPT_DIR/bin"
        API_DIR="$BIN_DIR/api"
        DEMO_DIR="$SCRIPT_DIR/demo"

        mkdir -p "$SL_LIB_DIR"

        CORE_ASSEMBLY_NAME="starlake-core_${SCALA_VERSION}-${NEW_SL_VERSION}-assembly.jar"
        CORE_ASSEMBLY_URL="$BASE_URL/${CORE_ASSEMBLY_NAME}"

        API_ZIP_NAME="starlake-api_${SCALA_VERSION}-${NEW_SL_VERSION}.zip"
        API_ZIP_URL="$BASE_URL/${API_ZIP_NAME}"

        # Delete old starlake core assembly
        rm -f "$SL_LIB_DIR"/starlake-core_*-assembly.jar

        # Download new core assembly
        echo "Downloading $CORE_ASSEMBLY_NAME to $SL_LIB_DIR..."
        get_binary_from_url "$CORE_ASSEMBLY_URL" "$SL_LIB_DIR/$CORE_ASSEMBLY_NAME"
        verify_sha256 "$SL_LIB_DIR/$CORE_ASSEMBLY_NAME" "$CORE_ASSEMBLY_URL.sha256"

        # Delete old api directory and reinstall from zip
        rm -rf "$API_DIR"

        echo "Downloading $API_ZIP_NAME to $BIN_DIR..."
        get_binary_from_url "$API_ZIP_URL" "$BIN_DIR/$API_ZIP_NAME"
        verify_sha256 "$BIN_DIR/$API_ZIP_NAME" "$API_ZIP_URL.sha256"

        echo "Extracting $API_ZIP_NAME..."
        unzip -q "$BIN_DIR/$API_ZIP_NAME" -d "$BIN_DIR"
        rm -f "$BIN_DIR/$API_ZIP_NAME"

        # Rename extracted directory to api (matches Setup.java behavior)
        EXTRACTED_DIR="$BIN_DIR/starlake-api-${NEW_SL_VERSION}"
        if [ -d "$EXTRACTED_DIR" ]; then
            echo "Renaming $EXTRACTED_DIR to $API_DIR"
            mv "$EXTRACTED_DIR" "$API_DIR"
        fi

        # Move demo zips out of api dir (matches Setup.java behavior)
        mkdir -p "$DEMO_DIR"
        if [ -f "$API_DIR/starbake.zip" ]; then
            mv "$API_DIR/starbake.zip" "$DEMO_DIR/starbake.zip"
        fi
        if [ -f "$API_DIR/tpch001.zip" ]; then
            mv "$API_DIR/tpch001.zip" "$DEMO_DIR/tpch001.zip"
        fi

        # Set API bin scripts as executable
        if [ -d "$API_DIR/bin" ]; then
            for file in "$API_DIR/bin"/local-*; do
                if [ -f "$file" ]; then
                    chmod +x "$file"
                fi
            done
        fi

        # Update python libs
        PYTHON_LIBS_BASE_URL="https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/python-libs"
        PYTHON_LIBS_DIR="${SL_PYTHON_LIBS_DIR:-$SCRIPT_DIR/bin/deps/python-libs}"
        echo "Updating python libs in $PYTHON_LIBS_DIR..."
        rm -rf "$PYTHON_LIBS_DIR"
        mkdir -p "$PYTHON_LIBS_DIR"
        get_binary_from_url "$PYTHON_LIBS_BASE_URL/versions.txt" "$PYTHON_LIBS_DIR/versions.txt"
        while IFS= read -r line; do
            line=$(echo "$line" | tr -d '[:space:]')
            if [ -n "$line" ] && [[ "$line" != \#* ]]; then
                echo "Downloading $line..."
                get_binary_from_url "$PYTHON_LIBS_BASE_URL/$line" "$PYTHON_LIBS_DIR/$line"
            fi
        done < "$PYTHON_LIBS_DIR/versions.txt"

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
