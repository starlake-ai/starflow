@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "SL_SCRIPT_DIR=%SCRIPT_DIR%"
if not defined SL_API_STARLAKE_CORE_PATH set "SL_API_STARLAKE_CORE_PATH=%SCRIPT_DIR%starlake.cmd"
set "API_BIN_DIR=%SCRIPT_DIR%\bin\api\bin"

if not defined SL_ROOT (
    set "SL_ROOT=%cd%"
)
set "SL_ROOT=!SL_ROOT:\=/!"

if not defined HADOOP_HOME (
    set "HADOOP_HOME=%SCRIPT_DIR%bin\hadoop"
)
set "PATH=%HADOOP_HOME%\bin;%PATH%"

if /i "%1" == "reinstall" (
    if exist "%SCRIPT_DIR%versions.cmd" del "%SCRIPT_DIR%versions.cmd"
    if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
) else (
    if exist "%SCRIPT_DIR%versions.cmd" (
        call "%SCRIPT_DIR%versions.cmd"
    )
)

if not defined SCALA_VERSION set "SCALA_VERSION=2.13"
set "SL_ARTIFACT_NAME=starlake-core_%SCALA_VERSION%"
set "SPARK_DIR_NAME=spark-%SPARK_VERSION%-bin-hadoop%HADOOP_VERSION%"
set "SPARK_TARGET_FOLDER=%SCRIPT_DIR%bin\spark"
set "SPARK_EXTRA_LIB_FOLDER=%SCRIPT_DIR%bin"
set "DEPS_EXTRA_LIB_FOLDER=%SPARK_EXTRA_LIB_FOLDER%\deps"
set "STARLAKE_EXTRA_LIB_FOLDER=%SPARK_EXTRA_LIB_FOLDER%\sl"
if not defined SL_DATASETS (
    set "SL_SQL_WH=%SL_ROOT%/datasets"
) else (
    set "SL_SQL_WH=%SL_DATASETS%"
)

if not defined SPARK_DRIVER_MEMORY set "SPARK_DRIVER_MEMORY=4g"
set "SL_MAIN=ai.starlake.job.Main"
if not defined SPARK_MASTER_URL set "SPARK_MASTER_URL=local[*]"
if not defined SL_PYTHON_LIBS_DIR set "SL_PYTHON_LIBS_DIR=%SCRIPT_DIR%bin\deps\python-libs"

if defined SL_VERSION (
    set "SL_JAR_NAME=%SL_ARTIFACT_NAME%-%SL_VERSION%-assembly.jar"
)

set "PROXY="
if defined https_proxy (
    set "PROXY=%https_proxy%"
) else if defined http_proxy (
    set "PROXY=%http_proxy%"
)

set "JAVA_ARGS="

if defined HTTPS_PROXY (
    echo Using HTTPS_PROXY: %HTTPS_PROXY%
    call :parse_proxy_and_build_args "https" "%HTTPS_PROXY%"
    if defined proxy_args set "JAVA_ARGS=%JAVA_ARGS% %proxy_args%"
)

if defined HTTP_PROXY (
    echo Using HTTP_PROXY: %HTTP_PROXY%
    call :parse_proxy_and_build_args "http" "%HTTP_PROXY%"
    if defined proxy_args set "JAVA_ARGS=%JAVA_ARGS% %proxy_args%"
)

if defined SPARK_DRIVER_OPTIONS (
    set "SPARK_DRIVER_OPTIONS=%SPARK_DRIVER_OPTIONS% %JAVA_ARGS%"
) else (
    set "SPARK_DRIVER_OPTIONS=%JAVA_ARGS%"
)

set "JAVA_OPTS=%JAVA_OPTS% %JAVA_ARGS%"

goto :handle_command

:parse_proxy_and_build_args
    REM Args: %1=type (http/https), %2=url
    REM Sets proxy_args variable with -Dtype.proxyHost/Port/User/Password flags
    set "proxy_args="
    set "_ptype=%~1"
    set "_purl=%~2"

    set "_tmpps=%TEMP%\sl_proxy_%RANDOM%.ps1"
    > "%_tmpps%" (
        echo $url = '%_purl%'
        echo if ($url -notmatch '://') { Write-Error "No protocol in proxy URL, assuming http://"; $url = 'http://' + $url }
        echo try { $uri = [uri]$url } catch { Write-Error "Cannot parse proxy URL: $url"; exit 0 }
        echo if (-not $uri.Host) { Write-Error "Warning: Could not parse %_ptype% proxy URL: $url"; exit 0 }
        echo $r = '-D%_ptype%.proxyHost=' + $uri.Host
        echo if ($uri.Port -gt 0) { $r += ' -D%_ptype%.proxyPort=' + [string]$uri.Port }
        echo if ($uri.UserInfo) {
        echo     $p = $uri.UserInfo.Split(':', 2)
        echo     $r += ' -D%_ptype%.proxyUser=' + [uri]::UnescapeDataString($p[0])
        echo     if ($p.Length -gt 1) { $r += ' -D%_ptype%.proxyPassword=' + [uri]::UnescapeDataString($p[1]) }
        echo }
        echo Write-Output $r
    )
    for /f "usebackq delims=" %%a in (`powershell -NoProfile -File "%_tmpps%"`) do set "proxy_args=%%a"
    del "%_tmpps%" 2>nul
    goto :eof

:get_binary_from_url
    set "url=%~1"
    set "target=%~2"
    if defined PROXY (
        if defined SL_INSECURE (
            curl --insecure --proxy "%PROXY%" --progress-bar -o "%target%" "%url%"
        ) else (
            curl --proxy "%PROXY%" --progress-bar -o "%target%" "%url%"
        )
    ) else (
        curl --progress-bar -o "%target%" "%url%"
    )
    if errorlevel 1 (
        echo Error: Failed to retrieve data from %url%.
        exit /b 1
    )
    exit /b 0

:launch_setup
    set "setup_url=https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.jar"
    echo Downloading %setup_url% to %SCRIPT_DIR%setup.jar
    call :get_binary_from_url "%setup_url%" "%SCRIPT_DIR%setup.jar"
    if errorlevel 1 exit /b 1

    set "RUNNER="
    if defined JAVA_HOME (
        set "RUNNER=%JAVA_HOME%\bin\java.exe"
    ) else (
        for %%X in (java.exe) do (set RUNNER=%%~$PATH:X)
        if not defined RUNNER (
            echo JAVA_HOME is not set and java not in PATH
            exit /b 1
        )
    )
    "%RUNNER%" -cp "%SCRIPT_DIR%setup.jar" Setup "%SCRIPT_DIR:~0,-1%" "windows"

    if exist "%API_BIN_DIR%" (
        for %%f in ("%API_BIN_DIR%\local-*") do (
            rem In Windows, .bat/.cmd files are executable by default.
            echo Granting execute permission to %%f is not necessary on Windows.
        )
    )
goto :eof

:launch_starlake
    if not exist "%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" (
        echo Starlake jar %SL_JAR_NAME% does not exist. Please install it.
        exit /b 1
    )

    if defined SL_LOG_LEVEL (
        if /i not "%SL_LOG_LEVEL%" == "error" (
            echo - JAVA_HOME=%JAVA_HOME%
            echo - SL_ROOT=%SL_ROOT%
        )
    )

    if defined SL_ENV (
        echo - SL_ENV=%SL_ENV%
    )

    if defined SL_DEBUG (
        set "SPARK_DRIVER_OPTIONS=%SPARK_DRIVER_OPTIONS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    )

    set "SL_RUN_MODE="
    if /i "%1" == "import" set SL_RUN_MODE=main
    if /i "%1" == "xls2yml" set SL_RUN_MODE=main
    if /i "%1" == "yml2xls" set SL_RUN_MODE=main

    if /i "%SL_RUN_MODE%" == "main" (
        set "SL_ROOT=!SL_ROOT!"
        java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Dlog4j.configurationFile="%SPARK_TARGET_FOLDER%/conf/log4j2.properties" -cp "%SPARK_TARGET_FOLDER%\jars\*;%DEPS_EXTRA_LIB_FOLDER%\*;%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" %SL_MAIN% %*
    ) else (
        set "extra_classpath=%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%"
        set "extra_jars=%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%"
        set "SPARK_SUBMIT=%SPARK_TARGET_FOLDER%\bin\spark-submit.cmd"

        set "deps_jars="
        for %%f in ("%DEPS_EXTRA_LIB_FOLDER%\*.jar") do (
            if defined deps_jars (
                call set "deps_jars=%%deps_jars%%,%%f"
            ) else (
                set "deps_jars=%%f"
            )
        )

        if /i "%SPARK_MASTER_URL:~0,5%" == "local" (
            if defined deps_jars (
                set "extra_classpath=!extra_classpath!;!deps_jars:,=;!"
            )
            set "SPARK_LOCAL_HOSTNAME=127.0.0.1"
            set "SPARK_HOME=%SCRIPT_DIR%bin\spark"
            set "SL_ROOT=!SL_ROOT!"
            call "!SPARK_SUBMIT!" %SPARK_EXTRA_PACKAGES% --driver-java-options "!SPARK_DRIVER_OPTIONS!" %SPARK_CONF_OPTIONS% --driver-class-path "!extra_classpath!" --class "%SL_MAIN%" --master "%SPARK_MASTER_URL%" "%SPARK_TARGET_FOLDER%\README.md" %*
        ) else (
            if defined deps_jars (
                set "extra_classpath=!deps_jars:,=;!"
                set "extra_jars=!extra_jars!,!deps_jars!"
            )
            set "SPARK_HOME=%SCRIPT_DIR%bin\spark"
            set "SL_ROOT=!SL_ROOT!"
            call "!SPARK_SUBMIT!" %SPARK_EXTRA_PACKAGES% %SPARK_CONF_OPTIONS% --driver-java-options "!SPARK_DRIVER_OPTIONS!" --driver-class-path "!extra_classpath!" --class "%SL_MAIN%" --master "%SPARK_MASTER_URL%" --jars "!extra_jars!" "%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" %*
        )
    )
goto :eof

:handle_command
    if /i "%1" == "--version" goto :version_command
    if /i "%1" == "version" goto :version_command
    if /i "%1" == "install" goto :install_command
    if /i "%1" == "reinstall" goto :install_command
    if /i "%1" == "serve" goto :serve_command
    if /i "%1" == "upgrade" goto :upgrade_command
    if /i "%1" == "_do_upgrade" goto :do_upgrade_command
    goto :default_command

:version_command
    echo Starlake %SL_VERSION%
    echo Duckdb JDBC driver %DUCKDB_VERSION%
    echo BigQuery Spark connector %SPARK_BQ_VERSION%
    echo Hadoop for Azure %HADOOP_AZURE_VERSION%
    echo Azure Storage %AZURE_STORAGE_VERSION%
    echo Spark %SPARK_VERSION%
    echo Hadoop %HADOOP_VERSION%
    echo Snowflake Spark connector %SPARK_SNOWFLAKE_VERSION%
    echo Snowflake JDBC driver %SNOWFLAKE_JDBC_VERSION%
    echo Postgres JDBC driver %POSTGRESQL_VERSION%
    echo AWS SDK %AWS_JAVA_SDK_VERSION%
    echo Hadoop for AWS %HADOOP_AWS_VERSION%
    echo Redshift JDBC driver %REDSHIFT_JDBC_VERSION%
    echo Redshift Spark connector %SPARK_REDSHIFT_VERSION%
    goto :eof

:select_starlake_version
    echo Fetching available versions...

    set "temp_meta=%TEMP%\sl_metadata_%RANDOM%.xml"
    call :get_binary_from_url "https://repo1.maven.org/maven2/ai/starlake/starlake-core_%SCALA_VERSION%/maven-metadata.xml" "%temp_meta%"
    if exist "%temp_meta%" (
         for /f "usebackq tokens=*" %%v in (`powershell -Command "[xml]$xml = Get-Content '%temp_meta%'; $versions = $xml.metadata.versioning.versions.version | Where-Object { $_ -match '^\d+\.\d+\.\d+$' } | Sort-Object { [version]$_ } -Descending; $latest = $versions[0]; $parts = $latest.Split('.'); $snapshot = $parts[0] + '.' + $parts[1] + '.' + ([int]$parts[2] + 1) + '-SNAPSHOT'; Write-Output $snapshot; $versions | Select-Object -First 5"`) do (
             if not defined SNAPSHOT_VERSION (
                 set "SNAPSHOT_VERSION=%%v"
             ) else (
                 set "LATEST_RELEASE_VERSIONS=!LATEST_RELEASE_VERSIONS! %%v"
             )
         )
         del "%temp_meta%"
    )

    set "VERSIONS=%SNAPSHOT_VERSION% %LATEST_RELEASE_VERSIONS%"

    :ask_version
    echo Last 5 available versions:
    for %%v in (%VERSIONS%) do echo %%v

    set "DEFAULT_VERSION=%SNAPSHOT_VERSION%"
    set /p "NEW_SL_VERSION=Which version do you want to install? [%DEFAULT_VERSION%]: "
    if not defined NEW_SL_VERSION set "NEW_SL_VERSION=%DEFAULT_VERSION%"

    echo Selected version: %NEW_SL_VERSION%
    goto :eof

:upgrade_command
    REM Self-update: download latest starlake.cmd and re-launch
    echo Updating starlake script...
    call :get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.cmd" "%SCRIPT_DIR%starlake.cmd.tmp"
    REM Ensure CRLF line endings
    powershell -Command "$c = [IO.File]::ReadAllText('%SCRIPT_DIR%starlake.cmd.tmp'); $c = $c -replace \"`r`n\",\"`n\" -replace \"`n\",\"`r`n\"; [IO.File]::WriteAllText('%SCRIPT_DIR%starlake.cmd.tmp', $c)"
    copy /y "%SCRIPT_DIR%starlake.cmd.tmp" "%SCRIPT_DIR%starlake.cmd" >nul
    del "%SCRIPT_DIR%starlake.cmd.tmp" 2>nul
    REM Re-launch with updated script
    "%SCRIPT_DIR%starlake.cmd" _do_upgrade
    goto :eof

:do_upgrade_command
    call :select_starlake_version
    if defined NEW_SL_VERSION (
        if exist "%SCRIPT_DIR%versions.cmd" (
             powershell -Command "(Get-Content '%SCRIPT_DIR%versions.cmd') -replace 'SL_VERSION=.*', 'SL_VERSION=%NEW_SL_VERSION%' | Set-Content '%SCRIPT_DIR%versions.cmd'"
             echo Updated versions.cmd with SL_VERSION=%NEW_SL_VERSION%
        )
        set "SL_VERSION=%NEW_SL_VERSION%"
        echo Upgrading Starlake to %NEW_SL_VERSION%...

        echo %NEW_SL_VERSION% | findstr /C:"SNAPSHOT" >nul
        if not errorlevel 1 (
             set "BASE_URL=https://central.sonatype.com/repository/maven-snapshots/ai/starlake"
        ) else (
             set "BASE_URL=https://repo1.maven.org/maven2/ai/starlake"
        )

        set "SL_LIB_DIR=%STARLAKE_EXTRA_LIB_FOLDER%"
        set "BIN_DIR=%SCRIPT_DIR%bin"
        set "API_DIR=!BIN_DIR!\api"
        set "DEMO_DIR=%SCRIPT_DIR%demo"

        if not exist "!SL_LIB_DIR!" mkdir "!SL_LIB_DIR!"

        set "CORE_ASSEMBLY_NAME=starlake-core_%SCALA_VERSION%-%NEW_SL_VERSION%-assembly.jar"
        set "CORE_ASSEMBLY_URL=!BASE_URL!/starlake-core_%SCALA_VERSION%/%NEW_SL_VERSION%/!CORE_ASSEMBLY_NAME!"

        set "API_ZIP_NAME=starlake-api_%SCALA_VERSION%-%NEW_SL_VERSION%.zip"
        set "API_ZIP_URL=!BASE_URL!/starlake-api_%SCALA_VERSION%/%NEW_SL_VERSION%/!API_ZIP_NAME!"

        REM Delete old starlake core assembly
        del /q "!SL_LIB_DIR!\starlake-core_*-assembly.jar" 2>nul

        REM Download new core assembly
        echo Downloading !CORE_ASSEMBLY_NAME! to !SL_LIB_DIR!...
        call :get_binary_from_url "!CORE_ASSEMBLY_URL!" "!SL_LIB_DIR!\!CORE_ASSEMBLY_NAME!"

        REM Delete old api directory and reinstall from zip
        if exist "!API_DIR!" rmdir /s /q "!API_DIR!"

        echo Downloading !API_ZIP_NAME! to !BIN_DIR!...
        call :get_binary_from_url "!API_ZIP_URL!" "!BIN_DIR!\!API_ZIP_NAME!"

        echo Extracting !API_ZIP_NAME!...
        powershell -Command "Expand-Archive -Path '!BIN_DIR!\!API_ZIP_NAME!' -DestinationPath '!BIN_DIR!' -Force"
        del /q "!BIN_DIR!\!API_ZIP_NAME!" 2>nul

        REM Rename extracted directory to api (matches Setup.java behavior)
        set "EXTRACTED_DIR=!BIN_DIR!\starlake-api-%NEW_SL_VERSION%"
        if exist "!EXTRACTED_DIR!" (
            echo Renaming !EXTRACTED_DIR! to !API_DIR!
            rename "!EXTRACTED_DIR!" "api"
        )

        REM Move demo zips out of api dir (matches Setup.java behavior)
        if not exist "!DEMO_DIR!" mkdir "!DEMO_DIR!"
        if exist "!API_DIR!\starbake.zip" move /y "!API_DIR!\starbake.zip" "!DEMO_DIR!\starbake.zip" >nul
        if exist "!API_DIR!\tpch001.zip" move /y "!API_DIR!\tpch001.zip" "!DEMO_DIR!\tpch001.zip" >nul

        REM Update python libs
        set "PYTHON_LIBS_BASE_URL=https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/python-libs"
        set "PYTHON_LIBS_DIR=%SL_PYTHON_LIBS_DIR%"
        echo Updating python libs in !PYTHON_LIBS_DIR!...
        if exist "!PYTHON_LIBS_DIR!" rmdir /s /q "!PYTHON_LIBS_DIR!"
        mkdir "!PYTHON_LIBS_DIR!"
        call :get_binary_from_url "!PYTHON_LIBS_BASE_URL!/versions.txt" "!PYTHON_LIBS_DIR!\versions.txt"
        for /f "usebackq delims=" %%L in ("!PYTHON_LIBS_DIR!\versions.txt") do (
            set "_line=%%L"
            REM Strip leading/trailing whitespace and skip comments
            set "_line=!_line: =!"
            if defined _line (
                echo !_line! | findstr /r "^#" >nul 2>&1
                if errorlevel 1 (
                    echo Downloading !_line!...
                    call :get_binary_from_url "!PYTHON_LIBS_BASE_URL!/!_line!" "!PYTHON_LIBS_DIR!\!_line!"
                )
            )
        )

        echo Upgrade complete.
    )
    goto :eof

:install_command
    call :launch_setup
    echo.
    echo Installation done. You're ready to enjoy Starlake!
    echo If any errors happen during installation. Please try to install again or open an issue.
    goto :eof

:serve_command
    if defined SL_API_DEBUG (
        set "JAVA_OPTS=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED %JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    ) else (
        set "JAVA_OPTS=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED %JAVA_OPTS%"
    )
    call "%SCRIPT_DIR%bin\api\bin\local-run-api.bat" "%SCRIPT_DIR%" dummy
    goto :eof

:default_command
    if not defined SL_HTTP_PORT (
        call :launch_starlake %*
    ) else (
        if not defined SL_HTTP_HOST set "SL_HTTP_HOST=127.0.0.1"
        set "SL_SERVE_URI=http://%SL_HTTP_HOST%:%SL_HTTP_PORT%"
        for %%v in (validation run transform compile) do (
            set "log=!SL_ROOT!\out\%%v.log"
            if exist "!log!" del "!log!"
        )
        curl "%SL_SERVE_URI%?ROOT=!SL_ROOT!&PARAMS=%*"
        for %%v in (validation run transform compile) do (
            set "log=!SL_ROOT!\out\%%v.log"
            if exist "!log!" type "!log!"
        )
    )
    goto :eof


