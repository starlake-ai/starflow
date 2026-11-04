@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "SL_SELF=%~f0"
set "SL_SCRIPT_DIR=%SCRIPT_DIR%"
if not defined SL_API_STARLAKE_CORE_PATH set "SL_API_STARLAKE_CORE_PATH=%SCRIPT_DIR%starlake.cmd"
set "API_BIN_DIR=%SCRIPT_DIR%\bin\api\bin"

if not defined SL_ROOT (
    set "SL_ROOT=%cd%"
)
set "SL_ROOT=!SL_ROOT:\=/!"

rem Prefer the embedded JDK installed by setup.ps1 when JAVA_HOME is not set,
rem then put that java first on the SESSION PATH: some run paths invoke bare
rem `java`, and on corporate machines the system PATH often pins an older
rem default Java first (user PATH entries can never outrank the system PATH).
if not defined JAVA_HOME if exist "%SCRIPT_DIR%jdk\bin\java.exe" set "JAVA_HOME=%SCRIPT_DIR%jdk"
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "PATH=%JAVA_HOME%\bin;%PATH%"

if not defined HADOOP_HOME (
    set "HADOOP_HOME=%SCRIPT_DIR%bin\hadoop"
)
set "PATH=%HADOOP_HOME%\bin;%PATH%"

rem Record which version-pin variables were already set by the USER's
rem environment, BEFORE versions.cmd is loaded below. Setup.java treats any of
rem these env vars as an override of its compiled-in pins, and unlike Unix
rem (sourced versions.sh variables stay shell-local), EVERY cmd variable
rem reaches child processes. :launch_setup therefore scrubs the pins that came
rem only from versions.cmd, so a pinned setup.jar applies its OWN release's
rem defaults - otherwise upgrading 1.8.x -> 1.7.x kept SPARK_VERSION=4.1.3 in
rem the child env and 1.7.1's Setup 404ed downloading a Spark 4 dist that does
rem not exist under its Spark 3 URL scheme. SL_VERSION and the ENABLE_* flags
rem are deliberately NOT on this list: both are managed intentionally by this
rem script (reinstall/upgrade pinning and infer_enable_flags_from_deps).
set "SL_PIN_VARS=SCALA_VERSION SPARK_VERSION HADOOP_VERSION DUCKDB_VERSION SPARK_BQ_VERSION HADOOP_AZURE_VERSION AZURE_STORAGE_VERSION JETTY_VERSION SPARK_SNOWFLAKE_VERSION SNOWFLAKE_JDBC_VERSION POSTGRESQL_VERSION TRINODB_VERSION AWS_JAVA_SDK_VERSION AWS_JAVA_SDK_V2_VERSION HADOOP_AWS_VERSION REDSHIFT_JDBC_VERSION SPARK_REDSHIFT_VERSION CONFLUENT_VERSION FLIGHT_SQL_JDBC_VERSION"
for %%V in (%SL_PIN_VARS%) do if defined %%V set "SL_USERENV_%%V=1"

if /i "%1" == "reinstall" (
    rem Read the currently-pinned SL_VERSION (if any) before wiping versions.cmd,
    rem so install_command below can reinstall AT that version instead of
    rem falling back to "latest github release". Unlike starlake.sh, no
    rem explicit export is needed: variables set in this cmd.exe process are
    rem automatically part of the environment block child processes (java) see.
    if exist "%SCRIPT_DIR%versions.cmd" (
        call "%SCRIPT_DIR%versions.cmd"
    )
    rem Capture which connectors were actually installed BEFORE bin\deps is
    rem wiped below - reinstall is meant to heal a poisoned install back to a
    rem consistent state at the same SL_VERSION, not silently switch a
    rem selective install into an ENABLE_ALL one. See
    rem :infer_enable_flags_from_deps for why ENABLE_ALL=false is required
    rem too, not just the per-category flags.
    call :infer_enable_flags_from_deps
    if exist "%SCRIPT_DIR%versions.cmd" del "%SCRIPT_DIR%versions.cmd"
    if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
    if exist "%SCRIPT_DIR%bin\deps" rmdir /s /q "%SCRIPT_DIR%bin\deps"
    if exist "%SCRIPT_DIR%bin\sl" rmdir /s /q "%SCRIPT_DIR%bin\sl"
) else (
    if exist "%SCRIPT_DIR%versions.cmd" (
        call "%SCRIPT_DIR%versions.cmd"
    )
)

rem Launch-time consistency guard: rescue installs left poisoned by an older
rem starlake.cmd whose `upgrade` only swapped the core jar/API and never
rem touched bin\spark, or by a re-provision that was interrupted after
rem wiping bin\spark but before the download completed (bin\spark absent
rem entirely - just as inconsistent, and just as silent otherwise). Gate on
rem versions.cmd existing: a genuinely fresh, never-installed tree has no
rem versions.cmd yet, and bin\spark not existing there is normal, not an
rem error. Skipped for the commands that are themselves how you fix this.
if not defined SL_SKIP_CONSISTENCY_CHECK if exist "%SCRIPT_DIR%versions.cmd" if defined SPARK_VERSION (
    set "_sl_guard_skip="
    if /i "%1" == "install" set "_sl_guard_skip=1"
    if /i "%1" == "reinstall" set "_sl_guard_skip=1"
    if /i "%1" == "upgrade" set "_sl_guard_skip=1"
    if /i "%1" == "_do_upgrade" set "_sl_guard_skip=1"
    if not defined _sl_guard_skip (
        set "_sl_guard_bad="
        if not exist "%SCRIPT_DIR%bin\spark\jars" set "_sl_guard_bad=1"
        if not exist "%SCRIPT_DIR%bin\spark\jars\spark-core_*-%SPARK_VERSION%.jar" set "_sl_guard_bad=1"
        if defined _sl_guard_bad (
            echo ERROR: Starflow installation is inconsistent.
            echo versions.cmd declares Spark %SPARK_VERSION% but %SCRIPT_DIR%bin\spark\jars has no matching spark-core jar ^(or is missing entirely - a re-provision may have been interrupted^).
            echo This usually happens after upgrading with an older starlake.cmd that did not refresh the Spark runtime, or an upgrade/reinstall that did not finish.
            echo Run "%SCRIPT_DIR%starlake.cmd" reinstall to fix it ^(wipes and re-downloads bin\spark, bin\deps and bin\sl for the currently pinned SL_VERSION^).
            echo Set SL_SKIP_CONSISTENCY_CHECK=1 to bypass this check.
            exit /b 1
        )
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

rem SL_RUN_MODE=main keeps the default heap of the JVM unless the user asks for one.
set "SL_USER_DRIVER_MEMORY=%SPARK_DRIVER_MEMORY%"
if not defined SPARK_DRIVER_MEMORY set "SPARK_DRIVER_MEMORY=4g"
set "SL_MAIN=ai.starlake.job.Main"
if not defined SPARK_MASTER_URL set "SPARK_MASTER_URL=local[*]"
if not defined SL_PYTHON_LIBS_DIR set "SL_PYTHON_LIBS_DIR=%SCRIPT_DIR%bin\deps\python-libs"

if defined SL_VERSION (
    set "SL_JAR_NAME=%SL_ARTIFACT_NAME%-%SL_VERSION%-assembly.jar"
)

rem Outside bin\: an archive built for another path is unusable.
set "APPCDS_DIR=%SCRIPT_DIR%.appcds"
set "APPCDS_ARCHIVE=%APPCDS_DIR%\starlake.jsa"
set "APPCDS_FINGERPRINT=%APPCDS_DIR%\starlake.fingerprint"
set "APPCDS_CONF_DIR=%APPCDS_DIR%\conf"
set "APPCDS_LAUNCH=%APPCDS_DIR%\starlake.launch"
set "APPCDS_CLASSPATH=%APPCDS_DIR%\starlake.classpath"
set "APPCDS_LOCK=%APPCDS_DIR%\build.lock"
set "APPCDS_FAILED=%APPCDS_DIR%\build.failed"

set "PROXY="
if defined https_proxy (
    set "PROXY=%https_proxy%"
) else if defined http_proxy (
    set "PROXY=%http_proxy%"
)

set "JAVA_ARGS="

REM The JVM prefers IPv4 by default; on dual-stack networks where an IPv4 path is
REM broken this makes it pick a dead address that curl/python would avoid.
REM "system" follows the OS address ordering and behaves identically on IPv4-only
REM networks. Users can still override via SPARK_DRIVER_OPTIONS/JAVA_OPTS.
echo.%JAVA_OPTS% %SPARK_DRIVER_OPTIONS% | findstr /C:"java.net.preferIPv6Addresses" >nul
if errorlevel 1 set "JAVA_ARGS=-Djava.net.preferIPv6Addresses=system"

if defined HTTPS_PROXY (
    echo Using HTTPS_PROXY: %HTTPS_PROXY%
    call :parse_proxy_and_build_args "https" "%HTTPS_PROXY%"
    if defined proxy_args set "JAVA_ARGS=!JAVA_ARGS! !proxy_args!"
)

if defined HTTP_PROXY (
    echo Using HTTP_PROXY: %HTTP_PROXY%
    call :parse_proxy_and_build_args "http" "%HTTP_PROXY%"
    if defined proxy_args set "JAVA_ARGS=!JAVA_ARGS! !proxy_args!"
)

rem Launcher flags first: on a repeated -D the JVM keeps the last one, so the user's
rem options win.
set "SPARK_DRIVER_OPTIONS=%JAVA_ARGS% %SPARK_DRIVER_OPTIONS%"

set "JAVA_OPTS=%JAVA_OPTS% %JAVA_ARGS%"

goto :handle_command

:infer_enable_flags_from_deps
    rem Called by reinstall (top of file) and do_upgrade_command, BEFORE
    rem bin\deps is wiped/replaced, so the checks below see the connectors
    rem that were actually installed. Setup.java computes every ENABLE_X as
    rem `ENABLE_ALL || envIsTrueWithDefaultTrue(X)`, and ENABLE_ALL defaults
    rem to true when unset - so ENABLE_ALL itself must be forced false here,
    rem in addition to each per-category flag, or the per-category values
    rem below have no effect at all (they would just be OR'd away).
    rem
    rem NOTE: Setup.java's field is ENABLE_MARIADB but the env var it
    rem actually reads is ENABLE_MARIA (a pre-existing field/env-var name
    rem mismatch in Setup.java itself - not a typo here). Every other flag
    rem below has a matching field/env-var name (verified against every
    rem envIsTrueWithDefaultTrue("ENABLE_...") call in Setup.java).
    rem
    rem Two sources of truth, in priority order, mirroring starlake.sh's
    rem infer_enable_flags_from_deps (see inference-fix-report.md for the
    rem git-log evidence behind the split):
    rem   1. The OLD versions.cmd's recorded ENABLE_X value (already loaded
    rem      by the versions.cmd step above, captured into OLD_ENABLE_X
    rem      below before it gets overwritten) - trusted verbatim.
    rem   2. Otherwise, jar presence in bin\deps - but only for categories
    rem      known to exist in every install this old (ENABLE_FLIGHTSQL is
    rem      the one exception: introduced after ENABLE_ALL/BIGQUERY/etc,
    rem      so its absence from an old versions.cmd means "did not exist
    rem      yet", not "user opted out" - it is left completely undefined
    rem      so Setup.java's own default-true provisions it).
    rem
    rem Written as flat, unparenthesized "set" + "if exist ... set" pairs
    rem (no shared loop/subroutine over a variable jar-pattern list) so each
    rem line is trivially, individually correct - deliberately avoiding any
    rem clever variadic-argument batch construct this could not be tested
    rem against a real cmd.exe.
    set "OLD_ENABLE_BIGQUERY=%ENABLE_BIGQUERY%"
    set "OLD_ENABLE_AZURE=%ENABLE_AZURE%"
    set "OLD_ENABLE_SNOWFLAKE=%ENABLE_SNOWFLAKE%"
    set "OLD_ENABLE_REDSHIFT=%ENABLE_REDSHIFT%"
    set "OLD_ENABLE_POSTGRESQL=%ENABLE_POSTGRESQL%"
    rem ENABLE_MARIA is the odd one out: the *file key* versions.cmd persists
    rem is ENABLE_MARIADB (generateVersions()'s own naming, verified back to
    rem v1.6.0), but the *runtime key* Setup.java reads is ENABLE_MARIA - so
    rem the old recorded value must be read from ENABLE_MARIADB, not
    rem ENABLE_MARIA (which was never set by the earlier versions.cmd load).
    set "OLD_ENABLE_MARIA=%ENABLE_MARIADB%"
    set "OLD_ENABLE_TRINODB=%ENABLE_TRINODB%"
    set "OLD_ENABLE_KAFKA=%ENABLE_KAFKA%"
    set "OLD_ENABLE_DUCKDB=%ENABLE_DUCKDB%"
    set "OLD_ENABLE_FLIGHTSQL=%ENABLE_FLIGHTSQL%"

    set "ENABLE_ALL=false"

    if defined OLD_ENABLE_BIGQUERY (set "ENABLE_BIGQUERY=%OLD_ENABLE_BIGQUERY%") else (
        set "ENABLE_BIGQUERY=false"
        if exist "%SCRIPT_DIR%bin\deps\spark-*bigquery*.jar" set "ENABLE_BIGQUERY=true"
    )

    if defined OLD_ENABLE_AZURE (set "ENABLE_AZURE=%OLD_ENABLE_AZURE%") else (
        set "ENABLE_AZURE=false"
        if exist "%SCRIPT_DIR%bin\deps\hadoop-azure-*.jar" set "ENABLE_AZURE=true"
    )

    if defined OLD_ENABLE_SNOWFLAKE (set "ENABLE_SNOWFLAKE=%OLD_ENABLE_SNOWFLAKE%") else (
        set "ENABLE_SNOWFLAKE=false"
        if exist "%SCRIPT_DIR%bin\deps\snowflake-jdbc-*.jar" set "ENABLE_SNOWFLAKE=true"
        if exist "%SCRIPT_DIR%bin\deps\spark-snowflake_*.jar" set "ENABLE_SNOWFLAKE=true"
    )

    if defined OLD_ENABLE_REDSHIFT (set "ENABLE_REDSHIFT=%OLD_ENABLE_REDSHIFT%") else (
        set "ENABLE_REDSHIFT=false"
        if exist "%SCRIPT_DIR%bin\deps\redshift-jdbc42-*.jar" set "ENABLE_REDSHIFT=true"
        if exist "%SCRIPT_DIR%bin\deps\spark-redshift_*.jar" set "ENABLE_REDSHIFT=true"
    )

    if defined OLD_ENABLE_POSTGRESQL (set "ENABLE_POSTGRESQL=%OLD_ENABLE_POSTGRESQL%") else (
        set "ENABLE_POSTGRESQL=false"
        if exist "%SCRIPT_DIR%bin\deps\postgresql-*.jar" set "ENABLE_POSTGRESQL=true"
    )

    if defined OLD_ENABLE_MARIA (set "ENABLE_MARIA=%OLD_ENABLE_MARIA%") else (
        set "ENABLE_MARIA=false"
        if exist "%SCRIPT_DIR%bin\deps\mariadb-java-client-*.jar" set "ENABLE_MARIA=true"
    )

    if defined OLD_ENABLE_TRINODB (set "ENABLE_TRINODB=%OLD_ENABLE_TRINODB%") else (
        set "ENABLE_TRINODB=false"
        if exist "%SCRIPT_DIR%bin\deps\trino-jdbc-*.jar" set "ENABLE_TRINODB=true"
    )

    if defined OLD_ENABLE_KAFKA (set "ENABLE_KAFKA=%OLD_ENABLE_KAFKA%") else (
        set "ENABLE_KAFKA=false"
        if exist "%SCRIPT_DIR%bin\deps\kafka-avro-serializer-*.jar" set "ENABLE_KAFKA=true"
        if exist "%SCRIPT_DIR%bin\deps\kafka-schema-registry-client-*.jar" set "ENABLE_KAFKA=true"
    )

    if defined OLD_ENABLE_DUCKDB (set "ENABLE_DUCKDB=%OLD_ENABLE_DUCKDB%") else (
        set "ENABLE_DUCKDB=false"
        if exist "%SCRIPT_DIR%bin\deps\duckdb_jdbc-*.jar" set "ENABLE_DUCKDB=true"
    )

    rem ENABLE_FLIGHTSQL is NOT on the legacy list: if the old versions.cmd
    rem never recorded it, leave it entirely undefined (not "false") so
    rem Setup.java's own default-true provisions it, same as a fresh install.
    if defined OLD_ENABLE_FLIGHTSQL (set "ENABLE_FLIGHTSQL=%OLD_ENABLE_FLIGHTSQL%") else (set "ENABLE_FLIGHTSQL=")
    goto :eof

:parse_proxy_and_build_args
    REM Args: %1=type (http/https), %2=url
    REM Sets proxy_args variable with -Dtype.proxyHost/Port/User/Password flags
    set "proxy_args="
    set "_ptype=%~1"
    set "_purl=%~2"

    set "SL_PS_PROXY_TYPE=%_ptype%"
    set "SL_PS_PROXY_URL=%_purl%"
    set "SL_PS_OUT=%TEMP%\sl_proxy_%RANDOM%%RANDOM%.txt"
    call :run_powershell proxy
    if exist "%SL_PS_OUT%" for /f "usebackq delims=" %%a in ("%SL_PS_OUT%") do set "proxy_args=%%a"
    del "%SL_PS_OUT%" 2>nul
    set "SL_PS_PROXY_URL="
    goto :eof

:get_binary_from_url
    set "url=%~1"
    set "target=%~2"
    if defined PROXY (
        if defined SL_INSECURE (
            curl -L --insecure --proxy "%PROXY%" --progress-bar -o "%target%" "%url%"
        ) else (
            curl -L --proxy "%PROXY%" --progress-bar -o "%target%" "%url%"
        )
    ) else (
        curl -L --progress-bar -o "%target%" "%url%"
    )
    if errorlevel 1 (
        echo Error: Failed to retrieve data from %url%.
        exit /b 1
    )
    exit /b 0

:verify_sha256
    set "vs_file=%~1"
    set "vs_url=%~2"
    call :get_binary_from_url "%vs_url%" "%vs_file%.sha256"
    if errorlevel 1 exit /b 1
    powershell -Command "$expected = (Get-Content '%vs_file%.sha256' -Raw).Trim().Split(' ')[0].ToLower(); $actual = (Get-FileHash -Algorithm SHA256 '%vs_file%').Hash.ToLower(); if ($actual -ne $expected) { exit 1 }"
    if errorlevel 1 (
        echo Error: checksum verification failed for %vs_file%
        exit /b 1
    )
    echo Checksum OK for %vs_file%
    del "%vs_file%.sha256" 2>nul
    goto :eof

:sync_spark_runtime
    rem %1: git ref (release tag or "master") whose Setup.java pins the wanted
    rem Spark version. Setup.java at that ref is the single source of truth
    rem for the release's Spark/Hadoop/connector version pins - it is also
    rem what generates versions.cmd on a fresh install. Fetch just its
    rem SPARK_VERSION default to decide whether bin\spark needs replacing,
    rem without duplicating those pins here. Setup.java itself only downloads
    rem Spark when bin\spark is ABSENT, so wiping a mismatched runtime here is
    rem exactly what makes the subsequent :launch_setup re-provision it.
    set "TARGET_SETUP_JAVA=%SCRIPT_DIR%.target-setup-java.tmp"
    call :get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starflow/%~1/src/main/java/Setup.java" "%TARGET_SETUP_JAVA%"
    if errorlevel 1 exit /b 1
    rem The regex pattern is passed base64-encoded (UTF8) rather than typed
    rem literally: it contains parens and embedded double-quote
    rem characters that are not protected by PowerShell's single-quote
    rem string (cmd.exe does not recognize single quotes as quoting at
    rem all), which would otherwise unbalance cmd own double-quote
    rem parity for this line or be misread as block syntax. The whole
    rem -Command argument stays a single physical line with exactly one
    rem opening and one closing double-quote, matching the already-working
    rem verify_sha256 -Command usage above.
    rem decoded pattern is: getEnv followed by open-paren quote SPARK_VERSION
    rem quote close-paren dot orElse open-paren quote capture-group quote close-paren
    set "TARGET_SPARK_VERSION="
    for /f "usebackq delims=" %%v in (`powershell -NoProfile -Command "$m = Select-String -Path '!TARGET_SETUP_JAVA!' -Pattern ([System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('Z2V0RW52XCgiU1BBUktfVkVSU0lPTiJcKVwub3JFbHNlXCgiKFteIl0qKSJcKQ=='))) | Select-Object -First 1; if ($m) { $m.Matches[0].Groups[1].Value }"`) do set "TARGET_SPARK_VERSION=%%v"
    del "%TARGET_SETUP_JAVA%" 2>nul

    if not defined TARGET_SPARK_VERSION (
        echo Warning: could not determine the target Spark version for %~1; re-provisioning bin\spark unconditionally to be safe.
        if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
    ) else (
        if exist "%SCRIPT_DIR%bin\spark\jars\spark-core_%SCALA_VERSION%-!TARGET_SPARK_VERSION!.jar" (
            echo Spark runtime already at !TARGET_SPARK_VERSION!, keeping bin\spark as-is.
        ) else (
            echo Spark runtime is changing ^(%SPARK_VERSION% -^> !TARGET_SPARK_VERSION!^): re-provisioning bin\spark.
            if exist "%SCRIPT_DIR%bin\spark" rmdir /s /q "%SCRIPT_DIR%bin\spark"
        )
    )
    exit /b 0

:launch_setup
    rem %1: optional git ref (tag, e.g. "v1.8.0") to fetch setup.jar from;
    rem defaults to "master". Install, reinstall and upgrade all pass the
    rem target release tag so Setup.java's compiled-in version defaults match
    rem that exact release. Only versions with no release tag to fetch from
    rem (SNAPSHOTs, local builds) fall back to master.
    set "_ls_ref=%~1"
    if "%_ls_ref%" == "" set "_ls_ref=master"
    set "setup_url=https://raw.githubusercontent.com/starlake-ai/starflow/%_ls_ref%/distrib/setup.jar"
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
    rem Scrub versions.cmd-loaded pins from the child environment (see the
    rem SL_PIN_VARS comment at the top of this script): the setup.jar fetched
    rem above must apply its OWN compiled-in defaults for Spark/Hadoop and
    rem every connector. Pins genuinely set in the user's environment before
    rem this script ran (captured in SL_USERENV_*) are kept as overrides.
    for %%V in (%SL_PIN_VARS%) do if not defined SL_USERENV_%%V set "%%V="

    "%RUNNER%" -cp "%SCRIPT_DIR%setup.jar" Setup "%SCRIPT_DIR:~0,-1%" "windows"
    if errorlevel 1 (
        echo Error: setup failed - the installation is incomplete.
        exit /b 1
    )

    if exist "%API_BIN_DIR%" (
        for %%f in ("%API_BIN_DIR%\local-*") do (
            rem In Windows, .bat/.cmd files are executable by default.
            echo Granting execute permission to %%f is not necessary on Windows.
        )
    )
goto :eof

rem AppCDS: an archive of parsed classes, memory-mapped by the JVM to cut startup. It
rem needs an empty SPARK_CONF_DIR, because the JVM refuses to dump or use an archive
rem with a non-empty directory on the classpath, and spark-submit puts the conf dir there.
rem
rem Paths given in driver options use forward slashes: Spark reads a backslash there as
rem an escape character, and C:\a\b would reach the JVM as C:ab.

rem What makes an archive unusable, written to a file that fc compares with the record of
rem a build. The fingerprint of a build is taken before it starts: a jar changed while it
rem runs still counts as a change.
:appcds_write_fingerprint
    > "%~1" (
        for %%j in (java.exe) do echo %%~$PATH:j
        if exist "%JAVA_HOME%\release" type "%JAVA_HOME%\release"
        for %%f in ("%SPARK_TARGET_FOLDER%\jars\*.jar" "%DEPS_EXTRA_LIB_FOLDER%\*.jar" "%STARLAKE_EXTRA_LIB_FOLDER%\*.jar") do echo %%~ff %%~zf %%~tf
    )
    exit /b 0

rem Exits with 0 when the jars or the JDK differ from the record of a build given as %1.
:appcds_changed_since
    setlocal
    set "_check=%TEMP%\sl-appcds-check-%RANDOM%%RANDOM%.tmp"
    call :appcds_write_fingerprint "%_check%"
    fc /b "%_check%" "%~1" >nul 2>&1
    set "_same=%errorlevel%"
    del "%_check%" 2>nul
    if "%_same%" == "0" (endlocal & exit /b 1)
    endlocal & exit /b 0

rem Anything but the logging config would be ignored once SPARK_CONF_DIR points elsewhere.
:spark_conf_is_default
    for %%f in ("%SPARK_TARGET_FOLDER%\conf\*") do (
        if /i not "%%~nxf" == "log4j2.properties" if /i not "%%~xf" == ".template" exit /b 1
    )
    exit /b 0

rem A build killed before releasing its lock must not block the next ones forever.
:clear_dead_appcds_lock
    if not exist "%APPCDS_LOCK%" exit /b 0
    powershell -NoProfile -NonInteractive -Command "if (((Get-Date) - (Get-Item -LiteralPath $env:APPCDS_LOCK).LastWriteTime).TotalMinutes -gt 10) { Remove-Item -LiteralPath $env:APPCDS_LOCK -Force }" >nul 2>&1
    exit /b 0

rem Rebuilds behind the command, in a window of its own that survives closing this one, and
rem does not hold the output of the command open. Exits with non-zero only when no rebuild can
rem happen: a read-only install, an install path with spaces, or a build that already failed
rem on these jars.
:start_appcds_rebuild
    copy /y nul "%APPCDS_DIR%\.writable" >nul 2>&1 || exit /b 1
    del "%APPCDS_DIR%\.writable" 2>nul
    if not "%SCRIPT_DIR: =%" == "%SCRIPT_DIR%" exit /b 1
    call :clear_dead_appcds_lock
    if exist "%APPCDS_LOCK%" exit /b 0
    call :appcds_changed_since "%APPCDS_FAILED%" || exit /b 1
    >&2 echo The jars or the JDK changed: rebuilding the AppCDS archive in the background.
    start "Starflow AppCDS" /min /low cmd /c ""%SL_SELF%" appcds-build > "%APPCDS_DIR%\build.log" 2>&1"
    exit /b 0

:enable_appcds
    if /i "%SL_APPCDS%" == "false" exit /b 0
    if not exist "%APPCDS_ARCHIVE%" exit /b 0
    if defined SPARK_CONF_DIR exit /b 0
    echo.%SPARK_DRIVER_OPTIONS%| findstr /c:"SharedArchiveFile" /c:"ArchiveClassesAtExit" /c:"-Xshare" >nul && exit /b 0
    call :spark_conf_is_default || exit /b 0
    call :appcds_changed_since "%APPCDS_FINGERPRINT%" || goto :enable_appcds_fresh
    call :start_appcds_rebuild || >&2 echo Warning: the AppCDS archive is out of date and could not be rebuilt automatically, startup will be slower. Run "%SL_SELF%" appcds-build to retry, or set SL_APPCDS=false to silence this.
    exit /b 0
:enable_appcds_fresh
    set "SPARK_CONF_DIR=%APPCDS_CONF_DIR%"
    set "_archive=%APPCDS_ARCHIVE:\=/%"
    set "_log_config=file:///%SPARK_TARGET_FOLDER:\=/%/conf/log4j2.properties"
    set "SPARK_DRIVER_OPTIONS=-Dlog4j.configurationFile=%_log_config% -XX:SharedArchiveFile=%_archive% -Xshare:auto -Xlog:cds=off -Xlog:cds+dynamic=off %SPARK_DRIVER_OPTIONS%"
    exit /b 0

rem On JDBC the database does the work and the JVM only orchestrates it, so C2 costs far more
rem than it gives back: compile time is most of the CPU of a short transform.
:enable_jdbc_c1
    if /i not "%~1" == "transform" exit /b 0
    if /i "%SL_JDBC_C1%" == "false" exit /b 0
    echo.%SPARK_DRIVER_OPTIONS%| findstr /c:"Tiered" >nul && exit /b 0
    call :run_powershell c1 >nul 2>&1 || exit /b 0
    set "SPARK_DRIVER_OPTIONS=-XX:TieredStopAtLevel=1 %SPARK_DRIVER_OPTIONS%"
    exit /b 0

rem What spark-submit gets as its driver classpath on a local master, in _driver_classpath.
:local_driver_classpath
    set "_driver_classpath=%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%"
    for %%f in ("%DEPS_EXTRA_LIB_FOLDER%\*.jar") do set "_driver_classpath=!_driver_classpath!;%%f"
    exit /b 0

rem Spark puts the driver options after its own -cp and the JVM keeps the last one. This replaces
rem the classpath of Spark, so it steps aside when Spark would add entries to it: the archive may
rem then go unused, but no entry goes missing.
:use_appcds_classpath
    if /i not "%SPARK_CONF_DIR%" == "%APPCDS_CONF_DIR%" exit /b 0
    if not exist "%APPCDS_CLASSPATH%" exit /b 0
    for %%s in ("%APPCDS_CLASSPATH%") do if %%~zs EQU 0 exit /b 0
    if defined HADOOP_CONF_DIR exit /b 0
    if defined YARN_CONF_DIR exit /b 0
    if defined SPARK_DIST_CLASSPATH exit /b 0
    echo.%SPARK_CONF_OPTIONS%| findstr /c:"driver-class-path" /c:"extraClassPath" >nul && exit /b 0
    set "_classpath_file=%APPCDS_CLASSPATH:\=/%"
    set "SPARK_DRIVER_OPTIONS=@%_classpath_file% %SPARK_DRIVER_OPTIONS%"
    exit /b 0

rem Training and its classpath do not depend on the environment of the caller.
:enter_appcds_training
    cd /d "%~1"
    set "SL_ENV="
    set "SPARK_CONF_OPTIONS="
    set "SPARK_EXTRA_PACKAGES="
    set "SL_DEBUG="
    set "SL_RUN_MODE="
    set "HADOOP_CONF_DIR="
    set "YARN_CONF_DIR="
    set "SPARK_DIST_CLASSPATH="
    set "SL_ROOT=%~1"
    set "SL_ROOT=!SL_ROOT:\=/!"
    set "SPARK_PRINT_LAUNCH_COMMAND=1"
    set "SPARK_MASTER_URL=local[*]"
    set "SPARK_CONF_DIR=%APPCDS_CONF_DIR%"
    rem Training only loads classes: no need for the heap of the user's jobs.
    set "SPARK_DRIVER_MEMORY=1g"
    exit /b 0

:build_appcds_archive
    if /i "%SL_APPCDS%" == "false" exit /b 0
    if /i "%SL_DRY_RUN%" == "true" exit /b 0
    if "%SCRIPT_DIR: =%" == "%SCRIPT_DIR%" goto :build_appcds_no_space
    >&2 echo Skipping the AppCDS archive: Spark driver options cannot quote an install path with spaces: %SCRIPT_DIR%
    exit /b 0
:build_appcds_no_space
    if not exist "%APPCDS_CONF_DIR%" mkdir "%APPCDS_CONF_DIR%"
    call :clear_dead_appcds_lock
    mkdir "%APPCDS_LOCK%" 2>nul && goto :build_appcds_locked_run
    echo An AppCDS archive build is already running.
    exit /b 0
:build_appcds_locked_run
    call :build_appcds_locked
    set "_build_status=%errorlevel%"
    rmdir "%APPCDS_LOCK%" 2>nul
    exit /b %_build_status%

:build_appcds_locked
    echo Building the AppCDS archive to speed up startup ^(set SL_APPCDS=false to skip^)...
    del /f /q "%APPCDS_ARCHIVE%.tmp" "%APPCDS_CLASSPATH%.tmp" "%APPCDS_LAUNCH%.tmp" >nul 2>&1
    call :appcds_write_fingerprint "%APPCDS_FINGERPRINT%.tmp"
    rem Train on a throwaway project that opens a Spark session: no user project exists at
    rem install time, and this trains nearly as well as a real one.
    set "_training=%TEMP%\sl-appcds-%RANDOM%%RANDOM%"
    mkdir "%_training%\metadata"
    > "%_training%\metadata\application.sl.yml" (
        echo version: 1
        echo application:
        echo   connectionRef: sparkLocal
        echo   connections:
        echo     sparkLocal:
        echo       type: fs
    )
    set "_archive_tmp=%APPCDS_ARCHIVE:\=/%.tmp"
    set "_log_config=file:///%SPARK_TARGET_FOLDER:\=/%/conf/log4j2.properties"
    set "_training_options=%JAVA_ARGS% -Dlog4j.configurationFile=%_log_config% -XX:ArchiveClassesAtExit=%_archive_tmp%"

    rem Spark lists slf4j-api before jars\*, so this jar comes twice once the JVM expands the
    rem wildcard. An archive records it once, and JDK 21 to 24 then reject it at startup, silently
    rem with -Xshare:auto. So the archive gets its own classpath, without the duplicate: the
    rem expansion by the JVM keeps its order, and a class found in two jars still comes from the
    rem same one.
    setlocal
    call :enter_appcds_training "%_training%"
    call :local_driver_classpath
    set "SPARK_HOME=%SCRIPT_DIR%bin\spark"
    call "%SPARK_TARGET_FOLDER%\bin\spark-submit.cmd" --driver-class-path "!_driver_classpath!" --class "%SL_MAIN%" --master "%SPARK_MASTER_URL%" --version > "%_training%\classpath.log" 2>&1
    set "SL_PS_LOG=%_training%\classpath.log"
    set "SL_PS_OUT=%APPCDS_CLASSPATH%.tmp"
    call :run_powershell classpath
    endlocal

    if not exist "%APPCDS_CLASSPATH%.tmp" goto :build_appcds_train
    set "_classpath_tmp=%APPCDS_CLASSPATH:\=/%.tmp"
    set "_training_options=@%_classpath_tmp% %_training_options%"
:build_appcds_train
    setlocal
    call :enter_appcds_training "%_training%"
    set "SPARK_DRIVER_OPTIONS=%_training_options%"
    call :launch_starlake validate > "%_training%\training.log" 2>&1
    endlocal & set "_training_status=%errorlevel%"
    cd /d "%SCRIPT_DIR%"

    if not "%_training_status%" == "0" goto :build_appcds_failed
    if not exist "%APPCDS_ARCHIVE%.tmp" goto :build_appcds_failed
    for %%s in ("%APPCDS_ARCHIVE%.tmp") do if %%~zs EQU 0 goto :build_appcds_failed
    rem The JVM creates its archives read-only, which on Windows blocks replacing them.
    attrib -r "%APPCDS_ARCHIVE%.tmp" >nul 2>&1
    if exist "%APPCDS_ARCHIVE%" attrib -r "%APPCDS_ARCHIVE%" >nul 2>&1

    set "SL_PS_LOG=%_training%\training.log"
    set "SL_PS_OUT=%APPCDS_LAUNCH%.tmp"
    set "SL_PS_TRAINING=%_training_options%"
    del "%APPCDS_LAUNCH%" 2>nul
    call :run_powershell record
    if exist "%APPCDS_LAUNCH%.tmp" call :move_when_free "%APPCDS_LAUNCH%.tmp" "%APPCDS_LAUNCH%"
    del "%APPCDS_FAILED%" 2>nul
    del "%APPCDS_CLASSPATH%" 2>nul
    if exist "%APPCDS_CLASSPATH%.tmp" call :move_when_free "%APPCDS_CLASSPATH%.tmp" "%APPCDS_CLASSPATH%"
    call :move_when_free "%APPCDS_ARCHIVE%.tmp" "%APPCDS_ARCHIVE%" || goto :build_appcds_in_use
    call :move_when_free "%APPCDS_FINGERPRINT%.tmp" "%APPCDS_FINGERPRINT%"
    rmdir /s /q "%_training%" 2>nul
    exit /b 0
:build_appcds_in_use
    >&2 echo Warning: could not replace the AppCDS archive, which a running command uses. Run "%SL_SELF%" appcds-build again once it has finished.
    del /f /q "%APPCDS_ARCHIVE%.tmp" "%APPCDS_FINGERPRINT%.tmp" >nul 2>&1
    rmdir /s /q "%_training%" 2>nul
    exit /b 1
:build_appcds_failed
    if exist "%_training%\training.log" powershell -NoProfile -NonInteractive -Command "Get-Content -LiteralPath $env:_training\training.log -Tail 5" 1>&2
    del /f /q "%APPCDS_ARCHIVE%.tmp" "%APPCDS_CLASSPATH%.tmp" >nul 2>&1
    move /y "%APPCDS_FINGERPRINT%.tmp" "%APPCDS_FAILED%" >nul
    rmdir /s /q "%_training%" 2>nul
    >&2 echo Warning: could not build the AppCDS archive, startup will be slower.
    exit /b 1

rem Unlike on Unix, a file that a running JVM maps cannot be replaced: waits up to a minute for it.
:move_when_free
    set "_move_tries=0"
:move_when_free_retry
    move /y "%~1" "%~2" >nul 2>&1 && exit /b 0
    set /a "_move_tries+=1"
    if %_move_tries% GEQ 30 exit /b 1
    ping -n 3 127.0.0.1 >nul
    goto :move_when_free_retry

rem Runs in a new process to read the versions.cmd that setup just wrote. Never fails the
rem install: the archive only makes startup faster.
:build_appcds_after_setup
    cmd /c ""%SL_SELF%" appcds-build"
    exit /b 0

rem The JVM of spark-submit without spark-submit: same classpath order, flags and archive. Only
rem what needs a Spark session may behave differently, so this is for the user to choose.
:launch_main
    set "_main_classpath_arg="
    set "_main_flags=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    if /i not "%SPARK_MASTER_URL:~0,5%" == "local" goto :launch_main_classpath
    if exist "%APPCDS_LAUNCH%" call :enable_appcds
    call :enable_jdbc_c1 %1
:launch_main_classpath
    if defined SPARK_CONF_DIR if /i "%SPARK_CONF_DIR%" == "%APPCDS_CONF_DIR%" goto :launch_main_archive
    call :local_driver_classpath
    set "_main_conf=%SPARK_CONF_DIR%"
    if not defined _main_conf set "_main_conf=%SPARK_TARGET_FOLDER%\conf"
    set "_main_classpath=!_driver_classpath!;%_main_conf%;%SPARK_TARGET_FOLDER%\jars\*"
    goto :launch_main_run
:launch_main_archive
    rem The classpath, then one JVM flag per line, as recorded from spark-submit.
    set "_main_classpath="
    set "_main_flags="
    for /f "usebackq delims=" %%l in ("%APPCDS_LAUNCH%") do (
        if not defined _main_classpath (set "_main_classpath=%%l") else (set "_main_flags=!_main_flags! %%l")
    )
    rem The expanded classpath is far longer than the 8191 characters of a cmd variable, so it stays
    rem in its file. It steps aside when entries must be added: the archive then goes unused, but no
    rem entry goes missing.
    if not exist "%APPCDS_CLASSPATH%" goto :launch_main_run
    if defined HADOOP_CONF_DIR goto :launch_main_run
    if defined YARN_CONF_DIR goto :launch_main_run
    if defined SPARK_DIST_CLASSPATH goto :launch_main_run
    set "_main_classpath_arg=@%APPCDS_CLASSPATH:\=/%"
:launch_main_run
    if defined HADOOP_CONF_DIR set "_main_classpath=!_main_classpath!;%HADOOP_CONF_DIR%"
    if defined YARN_CONF_DIR set "_main_classpath=!_main_classpath!;%YARN_CONF_DIR%"
    if defined SPARK_DIST_CLASSPATH set "_main_classpath=!_main_classpath!;%SPARK_DIST_CLASSPATH%"
    set "_main_memory="
    if defined SL_USER_DRIVER_MEMORY set "_main_memory=-Xmx%SL_USER_DRIVER_MEMORY%"
    set "SPARK_LOCAL_HOSTNAME=127.0.0.1"
    set "SPARK_HOME=%SPARK_TARGET_FOLDER%"
    set "_log_config=file:///%SPARK_TARGET_FOLDER:\=/%/conf/log4j2.properties"
    if not defined _main_classpath_arg set "_main_classpath_arg=-cp "!_main_classpath!""
    java !_main_flags! !_main_memory! -Dlog4j.configurationFile="!_log_config!" !SPARK_DRIVER_OPTIONS! !_main_classpath_arg! %SL_MAIN% %*
    exit /b %errorlevel%

rem The Hadoop that Spark bundles calls Subject.getSubject, which throws from JDK 24 on, and no JVM
rem flag brings it back. Without this, users only get a stack trace. Read from the release file of
rem the JDK to start no JVM: a JDK that has none gets no warning.
:warn_if_jdk_unsupported
    set "_jdk_home=%JAVA_HOME%"
    if not defined _jdk_home for /f "delims=" %%j in ('where java 2^>nul') do if not defined _jdk_home set "_jdk_home=%%~dpj.."
    set "_jdk_version="
    if exist "%_jdk_home%\release" for /f "tokens=2 delims==" %%v in ('findstr /b /c:"JAVA_VERSION=" "%_jdk_home%\release"') do set "_jdk_version=%%~v"
    if not defined _jdk_version exit /b 0
    set "_jdk_major="
    for /f "delims=._+-" %%m in ("%_jdk_version%") do set "_jdk_major=%%m"
    if not defined _jdk_major exit /b 0
    set /a "_jdk_major_number=_jdk_major" 2>nul
    if %_jdk_major_number% GEQ 24 >&2 echo Warning: Java %_jdk_major% cannot run Starflow: the Hadoop bundled with Spark fails on JDK 24 and newer. Set JAVA_HOME to JDK 17 or 21.
    exit /b 0

rem The block at the end of this file: what cmd cannot do by itself. A file of its own would have
rem to be downloaded by every install and upgrade.
:run_powershell
    set "SL_PS_ACTION=%~1"
    powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "$s=[IO.File]::ReadAllText($env:SL_SELF);$m='# SL_POWERSHELL_'+'BEGIN';iex $s.Substring($s.IndexOf($m))"
    exit /b %errorlevel%

:launch_starlake
    if not exist "%STARLAKE_EXTRA_LIB_FOLDER%\%SL_JAR_NAME%" (
        echo Starflow jar %SL_JAR_NAME% does not exist. Please install it.
        exit /b 1
    )

    call :warn_if_jdk_unsupported

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

    if /i "%1" == "import" set SL_RUN_MODE=main
    if /i "%1" == "xls2yml" set SL_RUN_MODE=main
    if /i "%1" == "yml2xls" set SL_RUN_MODE=main

    if /i "%SL_RUN_MODE%" == "main" (
        call :launch_main %*
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
            call :enable_appcds
            call :use_appcds_classpath
            call :enable_jdbc_c1 %1
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
    if /i "%1" == "appcds-build" goto :appcds_build_command
    goto :default_command

:version_command
    echo Starflow %SL_VERSION%
    echo Duckdb JDBC driver %DUCKDB_VERSION%
    echo BigQuery Spark connector %SPARK_BQ_VERSION%
    echo Hadoop for Azure %HADOOP_AZURE_VERSION%
    echo Azure Storage %AZURE_STORAGE_VERSION%
    echo Spark %SPARK_VERSION%
    echo Hadoop %HADOOP_VERSION%
    echo Snowflake Spark connector %SPARK_SNOWFLAKE_VERSION%
    echo Snowflake JDBC driver %SNOWFLAKE_JDBC_VERSION%
    echo Postgres JDBC driver %POSTGRESQL_VERSION%
    echo AWS SDK %AWS_JAVA_SDK_V2_VERSION%
    echo Hadoop for AWS %HADOOP_AWS_VERSION%
    echo Redshift JDBC driver %REDSHIFT_JDBC_VERSION%
    echo Redshift Spark connector %SPARK_REDSHIFT_VERSION%
    rem Explicitly: ending on goto :eof after a call loses the exit code when cmd /c runs this file,
    rem and a failed command would report success to whatever runs it.
    exit /b %errorlevel%

:select_starlake_version
    rem Non-interactive override: `upgrade --version X.Y.Z` / SL_UPGRADE_VERSION,
    rem so scripted/CI upgrades don't have to drive the interactive prompt.
    if not "%~1" == "" (
        set "NEW_SL_VERSION=%~1"
        rem %~1, not %NEW_SL_VERSION%: inside this parenthesized block, percent
        rem expansion happened at parse time, before the set above ran.
        echo Selected version: %~1 ^(forced^)
        goto :eof
    )

    echo Fetching available versions...

    set "temp_meta=%TEMP%\sl_releases_%RANDOM%.json"
    call :get_binary_from_url "https://api.github.com/repos/starlake-ai/starflow/releases?per_page=15" "%temp_meta%"
    if exist "%temp_meta%" (
         for /f "usebackq tokens=*" %%v in (`powershell -Command "$releases = Get-Content '%temp_meta%' -Raw | ConvertFrom-Json; $releases | ForEach-Object { $_.tag_name } | Where-Object { $_ -match '^v\d+\.\d+\.\d+$' } | ForEach-Object { $_.TrimStart('v') } | Sort-Object { [version]$_ } -Descending | Select-Object -First 5"`) do (
             set "LATEST_RELEASE_VERSIONS=!LATEST_RELEASE_VERSIONS! %%v"
             if not defined DEFAULT_VERSION set "DEFAULT_VERSION=%%v"
         )
         del "%temp_meta%"
    )

    if not defined DEFAULT_VERSION (
        echo Error: no releases found at https://github.com/starlake-ai/starflow/releases
        exit /b 1
    )

    set "VERSIONS=%LATEST_RELEASE_VERSIONS%"

    :ask_version
    echo Last 5 available versions:
    for %%v in (%VERSIONS%) do echo %%v

    set /p "NEW_SL_VERSION=Which version do you want to install? [%DEFAULT_VERSION%]: "
    if not defined NEW_SL_VERSION set "NEW_SL_VERSION=%DEFAULT_VERSION%"

    echo Selected version: %NEW_SL_VERSION%
    goto :eof

:upgrade_command
    REM Self-update: download latest starlake.cmd and re-launch, forwarding any
    REM extra args (e.g. --version X.Y.Z) through to _do_upgrade.
    echo Updating starlake script...
    call :get_binary_from_url "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/starlake.cmd" "%SCRIPT_DIR%starlake.cmd.tmp"
    REM Ensure CRLF line endings
    powershell -Command "$c = [IO.File]::ReadAllText('%SCRIPT_DIR%starlake.cmd.tmp'); $c = $c -replace \"`r`n\",\"`n\" -replace \"`n\",\"`r`n\"; [IO.File]::WriteAllText('%SCRIPT_DIR%starlake.cmd.tmp', $c)"
    copy /y "%SCRIPT_DIR%starlake.cmd.tmp" "%SCRIPT_DIR%starlake.cmd" >nul
    del "%SCRIPT_DIR%starlake.cmd.tmp" 2>nul
    REM Re-launch with updated script, dropping %1 ("upgrade") from the args
    "%SCRIPT_DIR%starlake.cmd" _do_upgrade %2 %3 %4 %5 %6 %7 %8 %9
    goto :eof

:do_upgrade_command
    rem Non-interactive version selection: `upgrade --version X.Y.Z` or
    rem SL_UPGRADE_VERSION env var. Falls back to the interactive prompt when
    rem neither is set (unchanged default).
    set "FORCED_SL_VERSION=%SL_UPGRADE_VERSION%"
    rem NOTE: `shift` followed by `%~1` must NEVER sit inside the same
    rem parenthesized ( ... ) block - cmd.exe substitutes every %1..%9 in a
    rem block once, at parse time, before the block's first line runs, so a
    rem shift part-way through would not affect a later %~1 read in that same
    rem block (a classic batch pitfall). Each branch below is therefore a
    rem separate unparenthesized line/label, so shift takes effect before the
    rem next line's %~1 is read.
    :du_parse_args
    if "%~1" == "" goto :du_parse_done
    if /i "%~1" == "--version" goto :du_version_flag
    set "_du_arg=%~1"
    if /i "!_du_arg:~0,10!" == "--version=" goto :du_version_eq
    if /i "%~1" == "--force" goto :du_force_flag
    if /i "%~1" == "--dry-run" goto :du_dry_run_flag
    shift
    goto :du_parse_args

    rem Separate unparenthesized labels for the same reason the --version branch is one:
    rem cmd.exe substitutes %1..%9 in a ( ... ) block at parse time, so a shift inside a
    rem block would not affect a later %~1 read in that same block.
    :du_force_flag
    set "SL_FORCE_DOWNLOAD=true"
    shift
    goto :du_parse_args

    :du_dry_run_flag
    set "SL_DRY_RUN=true"
    shift
    goto :du_parse_args

    :du_version_flag
    shift
    set "FORCED_SL_VERSION=%~1"
    shift
    goto :du_parse_args

    :du_version_eq
    set "FORCED_SL_VERSION=!_du_arg:~10!"
    shift
    goto :du_parse_args

    :du_parse_done

    call :select_starlake_version "%FORCED_SL_VERSION%"
    if defined NEW_SL_VERSION (
        echo Upgrading Starflow to %NEW_SL_VERSION%...
        set "TARGET_REF=v%NEW_SL_VERSION%"

        rem Wipe bin\spark if the target release pins a different Spark (see
        rem :sync_spark_runtime - shared with the install path).
        call :sync_spark_runtime "!TARGET_REF!"
        if errorlevel 1 exit /b 1

        rem bin\deps is always refreshed by launch_setup below: Setup.java
        rem deletes each dependency category by artefact-name match and
        rem re-downloads it at the target's pinned version, fixing stale
        rem connector jars even when Spark itself did not change.
        rem
        rem Unlike starlake.sh, versions.cmd's ENABLE_* values already reach
        rem the java subprocess automatically (Windows child processes
        rem inherit the full environment block, no export needed) - but
        rem Setup.java computes every ENABLE_X as
        rem `ENABLE_ALL || envIsTrueWithDefaultTrue(X)`, and ENABLE_ALL
        rem itself defaults to true when unset, which would short-circuit
        rem every category to true regardless of what versions.cmd says.
        rem infer_enable_flags_from_deps forces ENABLE_ALL=false and
        rem re-derives each per-category flag from which jars are actually
        rem present in bin\deps, so this still needs to run here too.
        call :infer_enable_flags_from_deps

        set "SL_VERSION=%NEW_SL_VERSION%"

        rem Re-provision via the real install machinery instead of a
        rem hand-rolled download of the core jar/API zip: Setup.java (fetched
        rem pinned to TARGET_REF) replaces bin\sl, bin\api and
        rem bin\deps\python-libs, only skips bin\spark when already present
        rem (handled above), and writes a brand new versions.cmd from scratch.
        call :launch_setup "!TARGET_REF!"
        if errorlevel 1 exit /b 1
        call :build_appcds_after_setup

        echo Upgrade complete.
    )
    goto :eof

:install_command
    rem --force / --dry-run. Set (not just local): Setup runs as a subprocess of
    rem :launch_setup and reads both from the environment, mirroring starlake.sh.
    rem   --force    re-download every dependency whatever is already on disk
    rem   --dry-run  print the plan and exit without downloading, deleting or
    rem              rewriting versions.cmd
    for %%A in (%*) do (
        if /i "%%~A" == "--force" set "SL_FORCE_DOWNLOAD=true"
        if /i "%%~A" == "--dry-run" set "SL_DRY_RUN=true"
    )
    rem Pin setup.jar to the release being installed whenever SL_VERSION names
    rem one. Setup.java's compiled-in defaults (Spark, Hadoop and every
    rem connector pin) are what get written to versions.cmd and provisioned
    rem into bin\, so they have to come from the SAME release as the core jar.
    rem Taking them from master instead can pair a core jar with a Spark it was
    rem never built against - installing 1.7.x (built for Spark 3.5) while
    rem master pins Spark 4. SL_VERSION here is either set by the caller
    rem (setup.ps1 sets it before calling `install`) or read from versions.cmd
    rem at the top of this script. Non-release values - SNAPSHOTs, locally
    rem built versions - have no release tag to fetch a setup.jar from, so they
    rem keep falling back to master. NOTE: no space before the pipe below, it
    rem would become part of the string findstr matches against.
    set "_is_ref="
    if not defined SL_VERSION goto :install_launch
    echo %SL_VERSION%|findstr /r /c:"^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*$" >nul
    if not errorlevel 1 set "_is_ref=v%SL_VERSION%"
    :install_launch
    set "_install_ref=%_is_ref%"
    if not defined _install_ref set "_install_ref=master"
    rem A version-changing `install` over an existing tree must wipe a Spark
    rem runtime that no longer matches the target release's pin (Setup.java
    rem only downloads Spark when bin\spark is absent - see
    rem :sync_spark_runtime). reinstall already wiped bin\spark at the top of
    rem this script, and a fresh tree has no runtime to check, so both skip
    rem the probe. The || form rather than a trailing errorlevel check: when
    rem the probe is skipped, findstr above may have left errorlevel 1 behind.
    if /i "%1" == "install" if exist "%SCRIPT_DIR%bin\spark" call :sync_spark_runtime "%_install_ref%" || exit /b 1
    call :launch_setup "%_install_ref%"
    if errorlevel 1 exit /b 1
    call :build_appcds_after_setup
    echo.
    echo Installation done. You're ready to enjoy Starflow!
    echo If any errors happen during installation. Please try to install again or open an issue.
    goto :eof

:appcds_build_command
    call :build_appcds_archive
    exit /b %errorlevel%

:serve_command
    if defined SL_API_DEBUG (
        set "JAVA_OPTS=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED %JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    ) else (
        set "JAVA_OPTS=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED %JAVA_OPTS%"
    )
    call "%SCRIPT_DIR%bin\api\bin\local-run-api.bat" "%SCRIPT_DIR%" dummy
    exit /b %errorlevel%

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

rem Nothing below is batch: :run_powershell reads it from here. No line may start with a colon.
goto :eof

# SL_POWERSHELL_BEGIN
$ErrorActionPreference = 'Stop'

# The value of the shallowest occurrence of a key in a YAML file. These helpers are no YAML
# parser: a layout they do not expect yields nothing, which keeps the default compiler.
function Get-YamlTopValue($key, $file) {
  if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { return '' }
  $seen = $false; $best = 0; $found = ''
  foreach ($raw in [IO.File]::ReadAllLines($file)) {
    $line = $raw.TrimStart(' ', "`t")
    $indent = $raw.Length - $line.Length
    if (-not $line.StartsWith("${key}:", [StringComparison]::Ordinal) -or ($seen -and $indent -ge $best)) { continue }
    $value = $line.Substring($key.Length + 1) -replace '[ \t]+#.*$', ''
    $seen = $true; $best = $indent; $found = $value.Trim(' ', "`t", '"', "'")
  }
  return $found
}

# Same rule as ConnectionInfo.getEngine: the JDBC engine is a jdbc type without sparkFormat.
function Test-YamlConnectionIsJdbc($name, $file) {
  if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { return $false }
  $inConnections = $false; $connectionsIndent = 0; $inConnection = $false
  $connectionIndent = 0; $fieldIndent = 0; $type = ''; $sparkFormat = $false
  foreach ($raw in [IO.File]::ReadAllLines($file)) {
    $line = $raw.TrimStart(' ', "`t")
    if ($line -eq '' -or $line.StartsWith('#')) { continue }
    $indent = $raw.Length - $line.Length
    $key = ($line -replace ':.*', '') -replace '["'']', ''
    if (-not $inConnections) {
      if ($key -ceq 'connections') { $inConnections = $true; $connectionsIndent = $indent }
      continue
    }
    if ($indent -le $connectionsIndent) { break }
    if (-not $inConnection) {
      if ($connectionIndent -eq 0) { $connectionIndent = $indent }
      if ($indent -eq $connectionIndent -and $key -ceq $name) { $inConnection = $true }
      continue
    }
    if ($indent -le $connectionIndent) { break }
    if ($fieldIndent -eq 0) { $fieldIndent = $indent }
    if ($indent -ne $fieldIndent) { continue }
    if ($key -ceq 'sparkFormat') { $sparkFormat = $true }
    if ($key -ceq 'type') { $type = (($line -replace '^[^:]*:', '') -replace '[ \t]+#.*$', '') -replace '[ \t"'']', '' }
  }
  return ($type.ToUpperInvariant() -ceq 'JDBC' -and -not $sparkFormat)
}

# Follows AutoTaskInfo.getRunConnectionRef with the project files, before any JVM starts.
# Anything that could send a task elsewhere answers no.
function Test-TransformRunsOnJdbc {
  if ("$env:SL_CONNECTION_REF$env:SL_ACTIVE_CONNECTION_REF$env:SL_TRANSFORM_CONNECTION_REF$env:SL_METADATA" -ne '') { return $false }
  $metadata = Join-Path $env:SL_ROOT 'metadata'
  $application = Join-Path $metadata 'application.sl.yml'
  $transform = Join-Path $metadata 'transform'
  if (Test-Path -LiteralPath $transform) {
    if (Get-ChildItem -LiteralPath $transform -Recurse -File | Select-String -SimpleMatch -Quiet 'connectionRef') { return $false }
  }
  $envName = $env:SL_ENV
  if (-not $envName) { $envName = Get-YamlTopValue 'SL_ENV' (Join-Path $metadata 'env.sl.yml') }
  $ref = Get-YamlTopValue 'transformConnectionRef' $application
  if (-not $ref) { $ref = Get-YamlTopValue 'connectionRef' $application }
  if ($ref -match '^\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$') {
    $name = $Matches[1]
    if ([Environment]::GetEnvironmentVariable($name)) { return $false }
    $ref = Get-YamlTopValue $name (Join-Path $metadata "env.$envName.sl.yml")
    if (-not $ref) { $ref = Get-YamlTopValue $name (Join-Path $metadata 'env.sl.yml') }
  }
  return ($ref -and (Test-YamlConnectionIsJdbc $ref $application))
}

# The words of the command spark-class really ran, as Spark prints them.
function Get-SparkCommandWords($log) {
  $line = Get-Content -LiteralPath $log | Where-Object { "$_".StartsWith('Spark Command: ') } | Select-Object -First 1
  if (-not $line) { return @() }
  return @($line -split '\s+')
}

# The classpath of Spark, expanded by the JVM and without duplicates.
function Write-AppcdsClasspath {
  $words = Get-SparkCommandWords $env:SL_PS_LOG
  $position = [Array]::IndexOf($words, '-cp')
  if ($position -lt 0) { return }
  $ErrorActionPreference = 'Continue'
  $listing = & java -cp $words[$position + 1] -XshowSettings:properties -version 2>&1 | ForEach-Object { "$_" }
  $entries = New-Object System.Collections.ArrayList
  $reading = $false
  foreach ($line in $listing) {
    if ($line -match '^\s*(\S+) = (.*)$') {
      $reading = ($Matches[1] -ceq 'java.class.path')
      if ($reading) { [void]$entries.Add($Matches[2].Trim()) }
    } elseif ($reading) { [void]$entries.Add($line.Trim()) }
  }
  $unique = @($entries | Where-Object { $_ } | Select-Object -Unique)
  if ($unique.Count -eq 0) { return }
  [IO.File]::WriteAllText($env:SL_PS_OUT, '-cp ' + (($unique -join ';') -replace '\\', '/') + "`r`n")
}

# Keeps what SL_RUN_MODE=main needs from the command spark-class really ran, so that it follows
# Spark upgrades: the classpath, then one JVM flag per line, without those of the training.
function Write-AppcdsLaunch {
  $words = Get-SparkCommandWords $env:SL_PS_LOG
  $position = [Array]::IndexOf($words, '-cp')
  if ($position -lt 0) { return }
  $skip = @{}
  foreach ($word in ($env:SL_PS_TRAINING -split '\s+')) { $skip[$word] = $true }
  $lines = New-Object System.Collections.ArrayList
  [void]$lines.Add($words[$position + 1])
  for ($i = $position + 2; $i -lt $words.Count -and $words[$i] -cne 'org.apache.spark.deploy.SparkSubmit'; $i++) {
    if (-not $skip.ContainsKey($words[$i]) -and $words[$i] -notmatch '^-Xmx') { [void]$lines.Add($words[$i]) }
  }
  [IO.File]::WriteAllLines($env:SL_PS_OUT, [string[]]$lines)
}

# The JVM flags of a proxy URL.
function Write-ProxyArgs {
  $type = $env:SL_PS_PROXY_TYPE
  $url = $env:SL_PS_PROXY_URL
  if ($url -notmatch '://') { $url = 'http://' + $url }
  try { $uri = [uri]$url } catch { [Console]::Error.WriteLine("Cannot parse $type proxy URL"); return }
  if (-not $uri.Host) { [Console]::Error.WriteLine("Could not parse $type proxy URL"); return }
  $flags = "-D$type.proxyHost=" + $uri.Host
  if ($uri.Port -gt 0) { $flags += " -D$type.proxyPort=" + [string]$uri.Port }
  if ($uri.UserInfo) {
    $parts = $uri.UserInfo.Split(':', 2)
    $flags += " -D$type.proxyUser=" + [uri]::UnescapeDataString($parts[0])
    if ($parts.Length -gt 1) { $flags += " -D$type.proxyPassword=" + [uri]::UnescapeDataString($parts[1]) }
  }
  [IO.File]::WriteAllText($env:SL_PS_OUT, $flags)
}

switch ($env:SL_PS_ACTION) {
  'c1' { try { if (Test-TransformRunsOnJdbc) { exit 0 } } catch { }; exit 1 }
  'classpath' { Write-AppcdsClasspath; exit 0 }
  'record' { Write-AppcdsLaunch; exit 0 }
  'proxy' { Write-ProxyArgs; exit 0 }
}
exit 2
