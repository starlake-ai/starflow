# Convert this script so it can run on Windows
function print_starlake_ascii_art {
    Write-Host "   _____  _______            _____   ______  _        ____  __          __"
    Write-Host "  / ____||__   __|    /\    |  __ \ |  ____|| |      / __ \ \ \        / /"
    Write-Host " | (___     | |      /  \   | |__) || |__   | |     | |  | | \ \  /\  / /"
    Write-Host "  \___ \    | |     / /\ \  |  _  / |  __|  | |     | |  | |  \ \/  \/ /"
    Write-Host "  ____) |   | |    / ____ \ | | \ \ | |     | |____ | |__| |   \  /\  /"
    Write-Host " |_____/    |_|   /_/    \_\|_|  \_\|_|     |______| \____/     \/  \/"
}

function get_installation_directory {
    $INSTALL_DIR = Read-Host "Where do you want to install Starflow? [$HOME\starlake]"
    if ($INSTALL_DIR -eq "") {
        $INSTALL_DIR = "$HOME\starlake"
    }
    $INSTALL_DIR = Invoke-Expression "Write-Output $INSTALL_DIR"
    New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null
    $INSTALL_DIR
}


function get_version_to_install {
    param([string]$RequestedVersion = "")

    # A forced --version=X.Y.Z is accepted directly - any released version, not
    # just the ones the interactive menu shows - matching setup.sh's behavior.
    # It short-circuits before the releases API call (no rate-limit exposure),
    # and only its release tag is verified (via the very setup.jar the pinned
    # install will fetch) so a typo fails fast here instead of halfway through
    # the install. Non release-shaped values (e.g. SNAPSHOTs) are passed
    # through untouched: they install from master's setup.jar, as on Linux.
    if ($RequestedVersion -ne "") {
        if ($RequestedVersion -match '^\d+\.\d+\.\d+$') {
            $tagUrl = "https://raw.githubusercontent.com/starlake-ai/starflow/v$RequestedVersion/distrib/setup.jar"
            try {
                Invoke-WebRequest -Method Head -Uri $tagUrl -UseBasicParsing | Out-Null
            } catch {
                Write-Host "Error: version $RequestedVersion not found (no tag v$RequestedVersion at https://github.com/starlake-ai/starflow/releases)"
                exit 1
            }
        }
        return $RequestedVersion
    }

    $RELEASE_VERSIONS = @()
    try {
        $releases = Invoke-RestMethod -Uri "https://api.github.com/repos/starlake-ai/starflow/releases?per_page=15" -UseBasicParsing
        $RELEASE_VERSIONS = @($releases |
            ForEach-Object { $_.tag_name } |
            Where-Object { $_ -match '^v\d+\.\d+\.\d+$' } |
            ForEach-Object { $_.TrimStart('v') } |
            Sort-Object -Descending { [version]$_ } |
            Select-Object -First 5)
    } catch {}

    if ($RELEASE_VERSIONS.Count -eq 0) {
        Write-Host "Error: no releases found at https://github.com/starlake-ai/starflow/releases"
        exit 1
    }

    $VERSIONS = $RELEASE_VERSIONS

    $DEFAULT_VERSION = $VERSIONS[0]

    $VERSION = $RequestedVersion
    while ($VERSION -notin $VERSIONS) {
        if ($VERSION -ne "") {
            Write-Host "Invalid version $VERSION. Please choose from the available versions."
        }
        Write-Host "Last available versions:"
        foreach ($v in $VERSIONS) { Write-Host "  $v" }
        $VERSION = Read-Host "Which version do you want to install? [$DEFAULT_VERSION]"
        if ($VERSION -eq "") {
            $VERSION = $DEFAULT_VERSION
        }
    }

    $VERSION
}

function install_starlake {
    param (
        [string]$INSTALL_DIR,
        [string]$VERSION
    )
    Write-Host "installing $VERSION"
    $url = "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/starlake.cmd"

    Write-Host "Downloading $url to $INSTALL_DIR"
    try {
        Invoke-WebRequest -Uri $url -OutFile "$INSTALL_DIR\starlake.cmd" -UseBasicParsing -ErrorAction Stop
        # Ensure CRLF line endings for Windows batch file
        $content = [System.IO.File]::ReadAllText("$INSTALL_DIR\starlake.cmd")
        $content = $content -replace "`r`n", "`n" -replace "`n", "`r`n"
        [System.IO.File]::WriteAllText("$INSTALL_DIR\starlake.cmd", $content)
    } catch {
        Write-Host "Error: Failed to download starlake.cmd from $url"
        Write-Host $_.Exception.Message
        exit 1
    }

    Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope Process
}

function add_starlake_to_path {
    param([string]$x)
    if (!($env:PATH -split ';' -contains $X)){
        $Env:Path+= ";" +  $x
        Write-Output $Env:Path
        $write = Read-Host 'Set PATH permanently ? (yes|no)'
        if ($write -eq "yes")
        {
            [Environment]::SetEnvironmentVariable("Path",$env:Path, [System.EnvironmentVariableTarget]::User)
            Write-Output 'PATH updated'
        }
    }
}

function run_installation_command {
    param([string]$InstallDir, [string]$Version)
    # Remove stale versions.cmd so setup.jar uses the correct SL_VERSION from the env
    if (Test-Path "$InstallDir\versions.cmd") {
        Remove-Item "$InstallDir\versions.cmd"
    }
    $env:SL_VERSION = $Version
    Start-Process -FilePath "$InstallDir\starlake.cmd" -ArgumentList 'install' -Wait -NoNewWindow
    if (Test-Path "$InstallDir\setup.jar") {
        Remove-Item "$InstallDir\setup.jar"
    }
}


function print_success_message {
    Write-Host "Starflow has been successfully installed!"
}

function get_java_major_version {
    # Parse the REAL runtime version from `java -version` (stderr). The file
    # version of java.exe is unreliable, and string comparison is lexicographic
    # ("8.0" -lt "11" is false), which used to let Java 8 pass the check.
    # Handles both version schemes: "17.0.12" -> 17, "1.8.0_292" -> 8.
    param([string]$JavaExe)
    if (-not (Test-Path $JavaExe) -and -not (Get-Command $JavaExe -ErrorAction SilentlyContinue)) {
        return 0
    }
    # `java -version` prints to STDERR. Under Windows PowerShell 5.1, when the
    # CALLER runs with $ErrorActionPreference = "Stop" (a dynamically scoped
    # preference this script inherits), redirected native stderr lines become
    # terminating NativeCommandError. Reset the preference for this scope so
    # the version probe can never throw; stderr lines may still arrive as
    # ErrorRecord objects, hence the explicit stringification below.
    $ErrorActionPreference = "Continue"
    $line = & $JavaExe -version 2>&1 | Select-Object -First 1
    if ("$line" -match 'version "(\d+)(?:\.(\d+))?') {
        $major = [int]$Matches[1]
        if ($major -eq 1 -and $Matches[2]) { $major = [int]$Matches[2] }
        return $major
    }
    return 0
}

function resolve_java {
    # JAVA_HOME wins when it is set (that is also what starlake.cmd executes);
    # otherwise fall back to `java` from the PATH.
    if ($env:JAVA_HOME) {
        $exe = Join-Path $env:JAVA_HOME "bin\java.exe"
        if (Test-Path $exe) {
            return @{ Exe = $exe; Major = (get_java_major_version $exe); Source = "JAVA_HOME ($env:JAVA_HOME)" }
        }
    }
    $cmd = Get-Command java -ErrorAction SilentlyContinue
    if ($cmd) {
        return @{ Exe = $cmd.Source; Major = (get_java_major_version $cmd.Source); Source = "PATH ($($cmd.Source))" }
    }
    return @{ Exe = ""; Major = 0; Source = "none" }
}

function get_required_java_version {
    # The java floor depends on the Starflow version being installed:
    #   up to 1.4.x (and every 0.x) -> java 11, from 1.5.0 on -> java 17.
    # Unparseable versions get the current floor (17).
    param([string]$SlVersion)
    if ($SlVersion -match '^(\d+)\.(\d+)') {
        $major = [int]$Matches[1]
        $minor = [int]$Matches[2]
        if ($major -lt 1 -or ($major -eq 1 -and $minor -le 4)) { return 11 }
    }
    return 17
}

function ensure_java {
    # Check the installed Java (JAVA_HOME first) against the floor required by
    # the Starflow version being installed. If none is found, or its version is
    # below that floor, install an EMBEDDED portable Temurin 17 JDK inside the
    # starlake install directory (<install-dir>\jdk) and update the SESSION
    # environment (JAVA_HOME + PATH). The embedded JDK is ALWAYS 17: it
    # satisfies both floors (a newer JVM runs older-target bytecode). No
    # administrator rights: portable zip + process-scoped variables only.
    # starlake.cmd picks the embedded JDK up automatically in later sessions.
    param([string]$InstallDir, [string]$SlVersion)

    $MinVersion = get_required_java_version $SlVersion
    $EmbeddedVersion = 17

    $java = resolve_java
    if ($java.Major -ge $MinVersion) {
        Write-Host "Using Java $($java.Major) from $($java.Source) (Starflow $SlVersion requires $MinVersion or above)"
        return
    }
    if ($java.Major -gt 0) {
        Write-Host "Java $($java.Major) found via $($java.Source) but Starflow $SlVersion requires Java $MinVersion or above."
    } else {
        Write-Host "No Java found (checked JAVA_HOME and PATH). Starflow $SlVersion requires Java $MinVersion or above."
    }

    $jdkDir = Join-Path $InstallDir "jdk"
    Write-Host "Installing an embedded Temurin $EmbeddedVersion JDK into $jdkDir (portable zip, no administrator rights)"
    # The Adoptium API redirects to the latest GA windows x64 JDK zip. On
    # Windows-on-ARM the x64 build runs fine under the built-in emulation.
    $adoptiumUrl = "https://api.adoptium.net/v3/binary/latest/$EmbeddedVersion/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"
    $zip = Join-Path ([System.IO.Path]::GetTempPath()) "starlake-embedded-jdk.zip"
    $unpack = Join-Path ([System.IO.Path]::GetTempPath()) ("starlake-jdk-" + [System.IO.Path]::GetRandomFileName())
    try {
        Invoke-WebRequest -UseBasicParsing -Uri $adoptiumUrl -OutFile $zip -ErrorAction Stop
    } catch {
        Write-Host "Error: failed to download the embedded JDK from $adoptiumUrl"
        Write-Host $_.Exception.Message
        exit 1
    }
    Expand-Archive -Path $zip -DestinationPath $unpack -Force
    Remove-Item $zip
    # the archive unpacks as jdk-<version>+<build>\ - flatten it to <install-dir>\jdk
    $inner = Get-ChildItem $unpack -Directory | Select-Object -First 1
    if ($null -eq $inner -or -not (Test-Path (Join-Path $inner.FullName "bin\java.exe"))) {
        Write-Host "Error: unexpected JDK archive layout"
        exit 1
    }
    if (Test-Path $jdkDir) { Remove-Item $jdkDir -Recurse -Force }
    Move-Item $inner.FullName $jdkDir
    Remove-Item $unpack -Recurse -Force

    # SESSION environment only: JAVA_HOME + PATH first, so this very install
    # (starlake.cmd install below) and everything started from this shell use
    # the embedded JDK. Later sessions are covered by starlake.cmd itself,
    # which adopts <install-dir>\jdk when JAVA_HOME is not set.
    $env:JAVA_HOME = $jdkDir
    $env:Path = (Join-Path $jdkDir "bin") + ";" + $env:Path

    $major = get_java_major_version (Join-Path $jdkDir "bin\java.exe")
    if ($major -lt $MinVersion) {
        Write-Host "Error: the embedded JDK did not install correctly (got version $major)"
        exit 1
    }
    Write-Host "Embedded JDK $major ready: JAVA_HOME=$jdkDir (session)"
}

function main {
    param([string[]]$ScriptArgs = @())
    $RequestedVersion = ""
    foreach ($arg in $ScriptArgs) {
        if ($arg.StartsWith("--version=")) {
            $RequestedVersion = $arg.Substring(10)
        }
    }
    print_starlake_ascii_art
    $INSTALL_DIR = get_installation_directory
    $VERSION = get_version_to_install -RequestedVersion $RequestedVersion
    # after version resolution: the java floor depends on the Starflow version
    # (<= 1.4 -> java 11, >= 1.5 -> java 17), and an embedded JDK would land
    # in <install-dir>\jdk
    ensure_java -InstallDir $INSTALL_DIR -SlVersion $VERSION
    install_starlake $INSTALL_DIR $VERSION
    add_starlake_to_path $INSTALL_DIR
    run_installation_command -InstallDir $INSTALL_DIR -Version $VERSION
    print_success_message
}

# Run the main function
main $args
