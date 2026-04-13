#!/usr/bin/env bash

set -e

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"

export SL_SCRIPT_DIR="$SCRIPT_DIR"
export SL_API_STARLAKE_CORE_PATH="${SL_API_STARLAKE_CORE_PATH:-$SCRIPT_DIR/starlake}"

API_BIN_DIR="$SCRIPT_DIR/bin/api/bin"

SL_ROOT="${SL_ROOT:-`pwd`}"

case "$1" in
  reinstall)
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
export SL_PYTHON_LIBS_DIR="${SL_PYTHON_LIBS_DIR:-$SCRIPT_DIR/bin/deps/python-libs}"
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
        local response=$(curl --insecure --proxy "$PROXY" --progress-bar -w "%{http_code}" -o "$target_file" "$url")
    else
        local response=$(curl --progress-bar -w "%{http_code}" -o "$target_file" "$url")
    fi
    local status_code=${response: -3}

    if [[ ! $status_code =~ ^(2|3)[0-9][0-9]$ ]]; then
        echo "Error: Failed to retrieve data from $url. HTTP status code: $status_code"
        exit 1
    fi
}

get_content_from_url() {
    local url=$1
    if [ -n "$PROXY" ] && [ -n "$SL_INSECURE" ]; then
        local response=$(curl --insecure --proxy "$PROXY" -s -w "%{http_code}" "$url")
    else
        local response=$(curl -s -w "%{http_code}" "$url")
    fi
    local status_code=${response: -3}

    if [[ ! $status_code =~ ^(2|3)[0-9][0-9]$ ]]; then
        echo "Error: Failed to retrieve data from $url. HTTP status code: $status_code" >&2
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
    ALL_RELEASE_VERSIONS=$(get_content_from_url https://repo1.maven.org/maven2/ai/starlake/starlake-core_${SCALA_VERSION}/maven-metadata.xml | awk -F'<|>' '/<version>/{print $3}' | grep -oE '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -rV)

    LATEST_RELEASE=$(echo "$ALL_RELEASE_VERSIONS" | head -n 1)
    SNAPSHOT_VERSION=$(echo "$LATEST_RELEASE" | awk -F. '{printf "%s.%s.%s-SNAPSHOT", $1, $2, $3+1}')
    LATEST_RELEASE_VERSIONS=$(echo "$ALL_RELEASE_VERSIONS" | head -n 5)

    VERSIONS=("$SNAPSHOT_VERSION" $LATEST_RELEASE_VERSIONS)

    menu_select "Select the version to install (use arrow keys):" "${VERSIONS[@]}"
    NEW_SL_VERSION="$SELECTED_OPTION"
    echo "Selected version: $NEW_SL_VERSION"
}

launch_setup() {
  local setup_url=https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.jar
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
    launch_setup
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

        if [[ "$NEW_SL_VERSION" == *"SNAPSHOT"* ]]; then
             BASE_URL="https://central.sonatype.com/repository/maven-snapshots/ai/starlake"
        else
             BASE_URL="https://repo1.maven.org/maven2/ai/starlake"
        fi

        SL_LIB_DIR="$STARLAKE_EXTRA_LIB_FOLDER"
        BIN_DIR="$SCRIPT_DIR/bin"
        API_DIR="$BIN_DIR/api"
        DEMO_DIR="$SCRIPT_DIR/demo"

        mkdir -p "$SL_LIB_DIR"

        CORE_ASSEMBLY_NAME="starlake-core_${SCALA_VERSION}-${NEW_SL_VERSION}-assembly.jar"
        CORE_ASSEMBLY_URL="$BASE_URL/starlake-core_${SCALA_VERSION}/${NEW_SL_VERSION}/${CORE_ASSEMBLY_NAME}"

        API_ZIP_NAME="starlake-api_${SCALA_VERSION}-${NEW_SL_VERSION}.zip"
        API_ZIP_URL="$BASE_URL/starlake-api_${SCALA_VERSION}/${NEW_SL_VERSION}/${API_ZIP_NAME}"

        # Delete old starlake core assembly
        rm -f "$SL_LIB_DIR"/starlake-core_*-assembly.jar

        # Download new core assembly
        echo "Downloading $CORE_ASSEMBLY_NAME to $SL_LIB_DIR..."
        get_binary_from_url "$CORE_ASSEMBLY_URL" "$SL_LIB_DIR/$CORE_ASSEMBLY_NAME"

        # Delete old api directory and reinstall from zip
        rm -rf "$API_DIR"

        echo "Downloading $API_ZIP_NAME to $BIN_DIR..."
        get_binary_from_url "$API_ZIP_URL" "$BIN_DIR/$API_ZIP_NAME"

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
        PYTHON_LIBS_DIR="$SL_PYTHON_LIBS_DIR"
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
