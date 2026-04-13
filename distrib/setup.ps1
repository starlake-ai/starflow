# Convert this script so it can run on Windows
function print_starlake_ascii_art {
    Write-Host "   _____ _______       _____  _               _  ________"
    Write-Host "  / ____|__   __|/\   |  __ \| |        /\   | |/ /  ____|"
    Write-Host " | (___    | |  /  \  | |__) | |       /  \  | ' /| |__"
    Write-Host "  \___ \   | | / /\ \ |  _  /| |      / /\ \ |  < |  __|"
    Write-Host "  ____) |  | |/ ____ \| | \ \| |____ / ____ \| . \| |____"
    Write-Host " |_____/   |_/_/    \_\_|  \_\______/_/    \_\_|\_\______|"
}

function get_installation_directory {
    $INSTALL_DIR = Read-Host "Where do you want to install Starlake? [$HOME\starlake]"
    if ($INSTALL_DIR -eq "") {
        $INSTALL_DIR = "$HOME\starlake"
    }
    $INSTALL_DIR = Invoke-Expression "Write-Output $INSTALL_DIR"
    New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null
    $INSTALL_DIR
}


function get_version_to_install {
    param([string]$RequestedVersion = "")

    $SCALA_VERSION = "2.13"

    $RELEASE_VERSIONS = @()
    try {
        $xml = [xml](Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/ai/starlake/starlake-core_$SCALA_VERSION/maven-metadata.xml" -UseBasicParsing).Content
        $RELEASE_VERSIONS = @($xml.metadata.versioning.versions.version |
            Where-Object { $_ -match '^\d+\.\d+\.\d+$' } |
            Sort-Object -Descending { [version]$_ } |
            Select-Object -First 5)
    } catch {}

    # Derive snapshot version by incrementing patch of latest release
    $SNAPSHOT_VERSION = $null
    if ($RELEASE_VERSIONS.Count -gt 0) {
        $v = [version]$RELEASE_VERSIONS[0]
        $SNAPSHOT_VERSION = "$($v.Major).$($v.Minor).$($v.Build + 1)-SNAPSHOT"
    }

    $VERSIONS = @()
    if ($SNAPSHOT_VERSION) { $VERSIONS += $SNAPSHOT_VERSION }
    $VERSIONS += $RELEASE_VERSIONS

    $DEFAULT_VERSION = if ($VERSIONS.Count -gt 0) { $VERSIONS[0] } else { $null }

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
    if ($VERSION -like "*SNAPSHOT*") {
        $url = "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.cmd"
    } else {
        $url = "https://raw.githubusercontent.com/starlake-ai/starlake/v$VERSION/distrib/starlake.cmd"
    }

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
    Write-Host "Starlake has been successfully installed!"
}

function check_java_version {
    # Check if Java is installed using JAVA_HOME env variable
    if ($null -eq $env:JAVA_HOME) {
        $runner = "java"
    } else {
        $runner = "$env:JAVA_HOME\bin\java"
    }
    $javaVersion = (Get-Command $runner | Select-Object -ExpandProperty Version).tostring()
    if ($null -eq $javaVersion) {
        Write-Host "Java is not installed. Please install Java 11 or above."
        exit 1
    }
    if ($javaVersion -lt "11") {
        Write-Host "Java version $javaVersion is not supported. Please install Java 11 or above."
        exit 1
    }
}

function main {
    param([string[]]$ScriptArgs = @())
    $RequestedVersion = ""
    foreach ($arg in $ScriptArgs) {
        if ($arg.StartsWith("--version=")) {
            $RequestedVersion = $arg.Substring(10)
        }
    }
    check_java_version
    print_starlake_ascii_art
    $INSTALL_DIR = get_installation_directory
    $VERSION = get_version_to_install -RequestedVersion $RequestedVersion
    install_starlake $INSTALL_DIR $VERSION
    add_starlake_to_path $INSTALL_DIR
    run_installation_command -InstallDir $INSTALL_DIR -Version $VERSION
    print_success_message
}

# Run the main function
main $args
