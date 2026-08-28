#!/bin/bash
set -e

if [ "$EUID" -eq 0 ]
  then echo "Please do not run this script as root or with sudo."
  exit
fi

print_starlake_ascii_art() {
    cat <<EOF
   _____  _______            _____   ______  _        ____  __          __
  / ____||__   __|    /\    |  __ \ |  ____|| |      / __ \ \ \        / /
 | (___     | |      /  \   | |__) || |__   | |     | |  | | \ \  /\  / /
  \___ \    | |     / /\ \  |  _  / |  __|  | |     | |  | |  \ \/  \/ /
  ____) |   | |    / ____ \ | | \ \ | |     | |____ | |__| |   \  /\  /
 |_____/    |_|   /_/    \_\|_|  \_\|_|     |______| \____/     \/  \/


EOF
}

get_installation_directory() {
    # Extract the version number from command-line arguments
    for arg in "$@"; do
        if [[ $arg == "--target="* ]]; then
            INSTALL_DIR="${arg#*=}"
        fi
    done
    if [[ -z "$INSTALL_DIR" ]]
    then
      read -p "Where do you want to install Starflow? [$HOME/starlake]: " INSTALL_DIR
      INSTALL_DIR=${INSTALL_DIR:-$HOME/starlake}
    fi
    INSTALL_DIR=$(eval "echo $INSTALL_DIR")
    mkdir -p "$INSTALL_DIR"
}

if [[ -n "${https_proxy}" ]] || [[ -n "${http_proxy}" ]]; then
  PROXY=${https_proxy:-$http_proxy}
elif [[ -n "${HTTPS_PROXY}" ]] || [[ -n "${HTTP_PROXY}" ]]; then
  PROXY=${HTTPS_PROXY:-$HTTP_PROXY}
fi

get_from_url() {
    local url=$1
    if [ -n "$PROXY" ] && [ -n "$SL_INSECURE" ]; then
        echo "Downloading data from $url using proxy $PROXY"
        local response=$(curl -L --insecure --proxy "$PROXY" -s -w "%{http_code}" "$url")
    else
        local response=$(curl -L -s -w "%{http_code}" "$url")
    fi
    local status_code=${response: -3}

    if [[ ! $status_code =~ ^(2|3)[0-9][0-9]$ ]]; then
        echo "Error: Failed to retrieve data from $url. HTTP status code: $status_code"
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

get_version_to_install() {
    # Extract the version number from command-line arguments
    for arg in "$@"; do
        if [[ $arg == "--version="* ]]; then
            VERSION="${arg#*=}"
        fi
    done

    if [[ -n "$VERSION" ]]; then
        return
    fi

    ALL_RELEASE_VERSIONS=$(get_from_url "https://api.github.com/repos/starlake-ai/starflow/releases?per_page=15" \
      | grep -o '"tag_name"[[:space:]]*:[[:space:]]*"v[0-9][^"]*"' \
      | sed -E 's/.*"v([^"]+)".*/\1/' \
      | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
      | sort -rV)

    if [[ -z "$ALL_RELEASE_VERSIONS" ]]; then
        echo "Error: no releases found at https://github.com/starlake-ai/starflow/releases" >&2
        exit 1
    fi

    LATEST_RELEASE_VERSIONS=$(echo "$ALL_RELEASE_VERSIONS" | head -n 5)

    VERSIONS=($LATEST_RELEASE_VERSIONS)

    menu_select "Which version do you want to install? (use arrow keys):" "${VERSIONS[@]}"
    VERSION="$SELECTED_OPTION"
    echo "Selected version: $VERSION"
}

install_starlake() {
    echo "installing $VERSION"
    local url=https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/starlake.sh
    get_from_url $url > "$INSTALL_DIR/starlake"
    chmod +x "$INSTALL_DIR/starlake"
}


add_starlake_to_path() {
    if [[ "$SHELL" == *zsh* ]] || [[ "$SHELL" == *bash* ]]; then
        if [[ "$SHELL" == *zsh* ]]; then
            if ! grep -q "$INSTALL_DIR" ~/.zshrc; then
                echo  >> ~/.zshrc
                if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
                    echo "export PATH=$INSTALL_DIR:\$PATH" >> ~/.zshrc
                fi
            fi
            zsh ~/.zshrc
        fi
        if [[ "$SHELL" == *bash* ]]; then
            if ! grep -q "$INSTALL_DIR" ~/.bashrc; then
                 echo  >> ~/.bashrc
                if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
                    echo "export PATH=$INSTALL_DIR:\$PATH" >> ~/.bashrc
                fi
            fi
            source ~/.bashrc
        fi
        echo "Starflow has been added to your PATH."
    else
        echo "Could not detect what shell you're using. Please add the following line to your shell configuration file manually:"
        echo "export PATH=$INSTALL_DIR:\$PATH"
    fi
}

run_installation_command() {
    SL_VERSION=$VERSION "$INSTALL_DIR/starlake" install
    #rm "$INSTALL_DIR/setup.jar"
}

print_success_message() {
    echo "Starflow has been successfully installed!"
}

get_binary_from_url() {
    local url=$1
    local target_file=$2
    local status_code
    if [ -n "$PROXY" ] && [ -n "$SL_INSECURE" ]; then
        echo "Downloading $url to $target_file using proxy $PROXY"
        status_code=$(curl -L --insecure --proxy "$PROXY" -s -w "%{http_code}" -o "$target_file" "$url")
    else
        status_code=$(curl -L -s -w "%{http_code}" -o "$target_file" "$url")
    fi
    if [[ ! $status_code =~ ^2[0-9][0-9]$ ]]; then
        echo "Error: Failed to download $url. HTTP status code: $status_code"
        exit 1
    fi
}

get_java_major_version() {
    # Parse the REAL runtime version from `java -version` (stderr), handling
    # both version schemes: "17.0.12" -> 17, "1.8.0_292" -> 8.
    # Prints 0 for missing/broken interpreters.
    local exe=$1
    local line major minor
    line=$("$exe" -version 2>&1 | head -n 1) || true
    if [[ "$line" =~ version\ \"([0-9]+)(\.([0-9]+))? ]]; then
        major="${BASH_REMATCH[1]}"
        minor="${BASH_REMATCH[3]}"
        if [[ "$major" == "1" && -n "$minor" ]]; then
            major="$minor"
        fi
        echo "$major"
    else
        echo 0
    fi
}

resolve_java() {
    # JAVA_HOME wins when it is set (that is also what the starlake launcher
    # executes); otherwise fall back to `java` from the PATH - including when
    # JAVA_HOME is set but points nowhere.
    RESOLVED_JAVA_MAJOR=0
    RESOLVED_JAVA_SOURCE="none"
    if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
        RESOLVED_JAVA_MAJOR=$(get_java_major_version "${JAVA_HOME}/bin/java")
        RESOLVED_JAVA_SOURCE="JAVA_HOME (${JAVA_HOME})"
        return
    fi
    if command -v java >/dev/null 2>&1; then
        RESOLVED_JAVA_MAJOR=$(get_java_major_version "$(command -v java)")
        RESOLVED_JAVA_SOURCE="PATH ($(command -v java))"
    fi
}

get_required_java_version() {
    # The java floor depends on the Starflow version being installed:
    #   up to 1.4.x (and every 0.x) -> java 11, from 1.5.0 on -> java 17.
    # Unparseable versions get the current floor (17).
    local sl_version=$1
    local major minor
    if [[ "$sl_version" =~ ^([0-9]+)\.([0-9]+) ]]; then
        major="${BASH_REMATCH[1]}"
        minor="${BASH_REMATCH[2]}"
        if [ "$major" -lt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -le 4 ]; }; then
            echo 11
            return
        fi
    fi
    echo 17
}

ensure_java() {
    # Check the installed Java (JAVA_HOME first) against the floor required by
    # the Starflow version being installed. If none is found, or its version is
    # below that floor, install an EMBEDDED portable Temurin 17 JDK inside the
    # starlake install directory ($INSTALL_DIR/jdk) and update the SESSION
    # environment (JAVA_HOME + PATH). The embedded JDK is ALWAYS 17: it
    # satisfies both floors (a newer JVM runs older-target bytecode). No root
    # rights: portable archive + process-scoped variables only. The starlake
    # launcher picks the embedded JDK up automatically in later sessions.
    local min_version embedded_version=17
    min_version=$(get_required_java_version "$VERSION")

    resolve_java
    if [ "$RESOLVED_JAVA_MAJOR" -ge "$min_version" ]; then
        echo "Using Java $RESOLVED_JAVA_MAJOR from $RESOLVED_JAVA_SOURCE (Starflow $VERSION requires $min_version or above)"
        return
    fi
    if [ "$RESOLVED_JAVA_MAJOR" -gt 0 ]; then
        echo "Java $RESOLVED_JAVA_MAJOR found via $RESOLVED_JAVA_SOURCE but Starflow $VERSION requires Java $min_version or above."
    else
        echo "No Java found (checked JAVA_HOME and PATH). Starflow $VERSION requires Java $min_version or above."
    fi

    local os arch
    case "$(uname -s)" in
        Darwin) os=mac ;;
        Linux)  os=linux ;;
        *) echo "Error: unsupported OS $(uname -s) - install Java $min_version manually."; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch=x64 ;;
        aarch64|arm64) arch=aarch64 ;;
        *) echo "Error: unsupported architecture $(uname -m) - install Java $min_version manually."; exit 1 ;;
    esac

    local jdk_dir="$INSTALL_DIR/jdk"
    echo "Installing an embedded Temurin $embedded_version JDK into $jdk_dir (portable archive, no root rights)"
    local adoptium_url="https://api.adoptium.net/v3/binary/latest/$embedded_version/ga/$os/$arch/jdk/hotspot/normal/eclipse?project=jdk"
    local archive unpack_dir top java_home_dir
    archive=$(mktemp -t starlake-jdk.XXXXXX).tar.gz
    unpack_dir=$(mktemp -d -t starlake-jdk.XXXXXX)
    get_binary_from_url "$adoptium_url" "$archive"
    tar -xzf "$archive" -C "$unpack_dir"
    rm -f "$archive"
    top=$(find "$unpack_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)
    # macOS archives nest the actual JDK under Contents/Home
    if [ -x "$top/Contents/Home/bin/java" ]; then
        java_home_dir="$top/Contents/Home"
    elif [ -x "$top/bin/java" ]; then
        java_home_dir="$top"
    else
        echo "Error: unexpected JDK archive layout"
        exit 1
    fi
    rm -rf "$jdk_dir"
    mv "$java_home_dir" "$jdk_dir"
    rm -rf "$unpack_dir"

    # SESSION environment only: JAVA_HOME + PATH first, so this very install
    # (starlake install below) uses the embedded JDK. Later sessions are
    # covered by the starlake launcher, which adopts $INSTALL_DIR/jdk when
    # JAVA_HOME is not set.
    export JAVA_HOME="$jdk_dir"
    export PATH="$JAVA_HOME/bin:$PATH"

    local major
    major=$(get_java_major_version "$jdk_dir/bin/java")
    if [ "$major" -lt "$min_version" ]; then
        echo "Error: the embedded JDK did not install correctly (got version $major)"
        exit 1
    fi
    echo "Embedded JDK $major ready: JAVA_HOME=$jdk_dir (session)"
}

main() {
    print_starlake_ascii_art
    get_installation_directory "$@"
    get_version_to_install "$@"
    # after version resolution: the java floor depends on the Starflow version
    # (<= 1.4 -> java 11, >= 1.5 -> java 17), and an embedded JDK would land
    # in $INSTALL_DIR/jdk
    ensure_java
    install_starlake
    add_starlake_to_path
    run_installation_command
    print_success_message
}

# Run the main function
main "$@"
