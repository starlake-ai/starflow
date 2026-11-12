# distrib/starlake.cmd on a real Windows, with a real release and a real JVM. Run by the Windows job
# of launcher-tests.yml. It installs as setup.ps1 does, with CRLF line endings, and counts the classes
# that really come from the archive, since the JVM ignores an archive it rejects without telling.
param([string]$InstallDir = 'C:\sl')

$launcherSource = (Resolve-Path (Join-Path $PSScriptRoot '../../../distrib/starlake.cmd')).Path
$launcher = Join-Path $InstallDir 'starlake.cmd'
$appcds = Join-Path $InstallDir '.appcds'
$archive = Join-Path $appcds 'starlake.jsa'
$lock = Join-Path $appcds 'build.lock'
$script:failures = 0

function Check($name, [bool]$condition, $detail = '') {
  if ($condition) { Write-Host "ok   $name" }
  else { $script:failures++; Write-Host "FAIL $name $detail" }
}

# Keeps its output in a file, and has a time limit: a command that hangs must say where it stopped.
function Starlake([string]$arguments, [hashtable]$environment = @{}, [string]$launcherPath = $launcher, [int]$minutes = 10) {
  foreach ($key in $environment.Keys) { Set-Item "Env:$key" $environment[$key] }
  $log = Join-Path $env:TEMP ("starlake-" + [Guid]::NewGuid() + '.log')
  # Process.Start, not Start-Process: the ExitCode of Start-Process -PassThru is empty when the
  # process exits before its handle is read, as a command refused by the JDK check can.
  $startInfo = New-Object System.Diagnostics.ProcessStartInfo 'cmd.exe', "/c `"`"$launcherPath`" $arguments > `"$log`" 2>&1`""
  $startInfo.UseShellExecute = $false
  $process = [System.Diagnostics.Process]::Start($startInfo)
  $finished = $process.WaitForExit($minutes * 60 * 1000)
  foreach ($key in $environment.Keys) { Remove-Item "Env:$key" -ErrorAction SilentlyContinue }
  $text = if (Test-Path $log) { Get-Content -Raw $log } else { '' }
  if (-not $finished) {
    Write-Host "TIMEOUT after $minutes minutes: starlake $arguments"
    Write-Host "--- output so far`n$text"
    Write-Host '--- processes'
    Get-CimInstance Win32_Process -Filter "Name='java.exe' or Name='cmd.exe' or Name='powershell.exe'" | ForEach-Object { "$($_.ProcessId) parent=$($_.ParentProcessId) $($_.CommandLine)" } | ForEach-Object { Write-Host ($_.Substring(0, [Math]::Min(400, $_.Length))) }
    Write-Host '--- install'
    Get-ChildItem $InstallDir -Force | ForEach-Object { Write-Host $_.Name }
    if (Test-Path $appcds) { Get-ChildItem $appcds -Force | ForEach-Object { Write-Host ".appcds\$($_.Name) $($_.Length)" } }
    Get-ChildItem $env:TEMP -Filter 'sl-appcds-*' -Directory -ErrorAction SilentlyContinue | ForEach-Object {
      Get-ChildItem $_.FullName -Filter '*.log' | ForEach-Object { Write-Host "--- $($_.FullName)"; Get-Content $_.FullName -Tail 40 | ForEach-Object { Write-Host $_ } }
    }
    exit 1
  }
  $process.WaitForExit()
  return [pscustomobject]@{ Code = $process.ExitCode; Text = $text }
}

function Classes-From-Archive([string]$arguments, [hashtable]$environment = @{}) {
  $log = Join-Path $env:TEMP ("class-load-" + [Guid]::NewGuid() + '.log')
  $cds = "$log.cds"
  $environment['SPARK_DRIVER_OPTIONS'] = '-Xlog:class+load:file=' + ($log -replace '\\', '/') + ' -Xlog:cds=info,cds+dynamic=info,class+path=info:file=' + ($cds -replace '\\', '/')
  $result = Starlake $arguments $environment
  $count = 0
  if (Test-Path $log) { $count = @(Select-String -Path $log -SimpleMatch 'shared objects file (top)').Count }
  $why = if (Test-Path $cds) { (Get-Content $cds | Where-Object { $_ -match 'mismatch|disabled|fail|error|not |unable|invalid' } | Select-Object -First 15) -join "`n" } else { 'no CDS log' }
  return [pscustomobject]@{ Count = $count; Code = $result.Code; Text = $result.Text; Why = $why }
}

function Wait-For-Rebuild {
  $deadline = (Get-Date).AddMinutes(6)
  Start-Sleep -Seconds 2
  while ((Test-Path $lock) -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 3 }
  return -not (Test-Path $lock)
}

function Write-Project($root, $application) {
  New-Item -ItemType Directory -Force -Path (Join-Path $root 'metadata\transform\dom') | Out-Null
  [IO.File]::WriteAllText((Join-Path $root 'metadata\application.sl.yml'), $application)
  [IO.File]::WriteAllText((Join-Path $root 'metadata\transform\dom\tt.sql'), 'SELECT 1 AS id')
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$content = [IO.File]::ReadAllText($launcherSource) -replace "`r`n", "`n" -replace "`n", "`r`n"
[IO.File]::WriteAllText($launcher, $content)

Write-Host '--- install'
$install = Starlake 'install' @{} $launcher 25
Write-Host $install.Text
Check 'install succeeds' ($install.Code -eq 0)
Check 'install builds the archive' (Test-Path $archive)
Check 'install records the classpath of the archive' (Test-Path (Join-Path $appcds 'starlake.classpath'))
Check 'install records the launch command' (Test-Path (Join-Path $appcds 'starlake.launch'))
Check 'install records the fingerprint' (Test-Path (Join-Path $appcds 'starlake.fingerprint'))
Check 'install releases the lock' (-not (Test-Path $lock))
if (-not (Test-Path $archive)) {
  $failed = Join-Path $appcds 'build.failed'
  Write-Host "No archive. build.failed present: $(Test-Path $failed)"
  Write-Host 'Stopping here: everything below needs the archive.'
  exit 1
}
Write-Host "the published archive is read-only: $((Get-Item $archive).IsReadOnly)"
Check 'the published archive can be replaced' (-not (Get-Item $archive).IsReadOnly)
$classpathFile = Get-Content -Raw (Join-Path $appcds 'starlake.classpath')
Check 'the classpath of the archive has no duplicate' (
  (($classpathFile -replace '^-cp ', '').Trim() -split ';' | Group-Object | Where-Object { $_.Count -gt 1 }).Count -eq 0)

$fsProject = Join-Path $env:TEMP 'sl-fs-project'
Write-Project $fsProject "version: 1`napplication:`n  connectionRef: sparkLocal`n  connections:`n    sparkLocal:`n      type: fs`n"
$jdbcProject = Join-Path $env:TEMP 'sl-jdbc-project'
Write-Project $jdbcProject ("version: 1`napplication:`n  connectionRef: duckdb`n  audit:`n    active: false`n  connections:`n    duckdb:`n      type: jdbc`n      options:`n        url: `"jdbc:duckdb:" + ($jdbcProject -replace '\\', '/') + "/duckdb.db`"`n        driver: `"org.duckdb.DuckDBDriver`"`n")
Set-Location $fsProject

Write-Host '--- the archive is really used'
$used = Classes-From-Archive 'validate'
Write-Host "classes from the archive on spark-submit: $($used.Count)"
Check 'spark-submit loads classes from the archive' ($used.Count -gt 1000) "count=$($used.Count) code=$($used.Code) $($used.Text)"
Check 'a command with the archive still succeeds' ($used.Code -eq 0) $used.Text
$main = Classes-From-Archive 'validate' @{ SL_RUN_MODE = 'main' }
Write-Host "classes from the archive on SL_RUN_MODE=main: $($main.Count)"
Check 'SL_RUN_MODE=main loads classes from the archive' ($main.Count -gt 1000) "count=$($main.Count) code=$($main.Code)`n--- CDS`n$($main.Why)`n--- launch file`n$((Get-Content (Join-Path $appcds 'starlake.launch')) -join "`n")"
Check 'SL_RUN_MODE=main succeeds' ($main.Code -eq 0) $main.Text
Check 'the logging config is understood' (-not $used.Text.Contains('Invalid URL')) $used.Text
Check 'the logging config is understood on SL_RUN_MODE=main' (-not $main.Text.Contains('Invalid URL'))
$off = Classes-From-Archive 'validate' @{ SL_APPCDS = 'false' }
Check 'SL_APPCDS=false does not use the archive' ($off.Count -eq 0 -and $off.Code -eq 0) "count=$($off.Count)"

$with = Measure-Command { Starlake 'validate' | Out-Null }
$without = Measure-Command { Starlake 'validate' @{ SL_APPCDS = 'false' } | Out-Null }
Write-Host ("validate: {0:n1}s with the archive, {1:n1}s without" -f $with.TotalSeconds, $without.TotalSeconds)

Write-Host '--- exit codes'
# Controls: scripts whose exit code is known, through the same function as the launcher.
$controls = Join-Path $env:TEMP 'sl-controls'
New-Item -ItemType Directory -Force -Path $controls | Out-Null
$plain = Join-Path $controls 'plain.cmd'
[IO.File]::WriteAllText($plain, "@echo off`r`nexit /b 7`r`n")
$called = Join-Path $controls 'called.cmd'
[IO.File]::WriteAllText($called, "@echo off`r`nsetlocal enabledelayedexpansion`r`ncall :sub`r`ngoto :eof`r`n:sub`r`n    exit /b 7`r`n")
$explicit = Join-Path $controls 'explicit.cmd'
[IO.File]::WriteAllText($explicit, "@echo off`r`nsetlocal enabledelayedexpansion`r`ncall :sub`r`nexit /b %errorlevel%`r`n:sub`r`n    exit /b 7`r`n")
$explicitResult = Starlake '' @{} $explicit
Write-Host "control call and exit /b: $($explicitResult.Code)"
Check 'a call followed by exit /b keeps the exit code' ($explicitResult.Code -eq 7)
$plainResult = Starlake '' @{} $plain
$calledResult = Starlake '' @{} $called
Write-Host "control exit /b 7: $($plainResult.Code)"
Write-Host "control call and goto :eof: $($calledResult.Code)"
Check 'the harness sees an exit code' ($plainResult.Code -eq 7)
$masterLauncher = Join-Path $InstallDir 'starlake-master.cmd'
curl.exe -fsSL -o $masterLauncher 'https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/starlake.cmd'
$masterContent = [IO.File]::ReadAllText($masterLauncher) -replace "`r`n", "`n" -replace "`n", "`r`n"
[IO.File]::WriteAllText($masterLauncher, $masterContent)
$masterResult = Starlake 'transform --name no.such_task' @{} $masterLauncher
Write-Host "failing transform through the launcher of master: $($masterResult.Code)"
Remove-Item $masterLauncher
$unknown = Starlake 'transform --name no.such_task' @{ SL_APPCDS = 'false' }
Write-Host "exit code of a failing transform: $($unknown.Code)"
Check 'a failing command exits non-zero' ($unknown.Code -ne 0) $unknown.Text
$unknownMain = Starlake 'transform --name no.such_task' @{ SL_APPCDS = 'false'; SL_RUN_MODE = 'main' }
Write-Host "exit code of a failing transform on SL_RUN_MODE=main: $($unknownMain.Code)"
Check 'a failing command on SL_RUN_MODE=main exits non-zero' ($unknownMain.Code -ne 0) $unknownMain.Text
$succeeding = Starlake 'validate' @{ SL_APPCDS = 'false' }
Check 'a succeeding command exits with 0' ($succeeding.Code -eq 0) "code=$($succeeding.Code)"

Write-Host '--- driver options'
$printed = Starlake 'validate' @{ SPARK_PRINT_LAUNCH_COMMAND = '1'; SPARK_DRIVER_OPTIONS = '-Dmy.option=1'; HTTP_PROXY = 'http://proxy.invalid:3128'; SL_APPCDS = 'false' }
$proxyAt = $printed.Text.IndexOf('-Dhttp.proxyHost')
$userAt = $printed.Text.IndexOf('-Dmy.option=1')
Check 'a command with a proxy set still runs' (-not $printed.Text.Contains('was unexpected')) $printed.Text
Check 'proxy flags reach the driver when the user sets options' ($proxyAt -ge 0) $printed.Text
Check 'the options of the user come after the flags of the launcher' ($userAt -gt $proxyAt)

Write-Host '--- C1 on JDBC'
Set-Location $jdbcProject
$jdbc = Starlake 'transform --name dom.tt' @{ SPARK_PRINT_LAUNCH_COMMAND = '1' }
Check 'a transform on JDBC uses C1' ($jdbc.Text.Contains('TieredStopAtLevel=1')) $jdbc.Text
Check 'a transform on JDBC with C1 and the archive succeeds' ($jdbc.Code -eq 0) $jdbc.Text
$noC1 = Starlake 'transform --name dom.tt' @{ SPARK_PRINT_LAUNCH_COMMAND = '1'; SL_JDBC_C1 = 'false' }
Check 'SL_JDBC_C1=false turns C1 off' (-not $noC1.Text.Contains('TieredStopAtLevel'))
$other = Starlake 'validate' @{ SPARK_PRINT_LAUNCH_COMMAND = '1' }
Check 'only transform uses C1' (-not $other.Text.Contains('TieredStopAtLevel'))
Set-Location $fsProject
$fs = Starlake 'transform --name dom.tt' @{ SPARK_PRINT_LAUNCH_COMMAND = '1' }
Check 'a transform on fs does not use C1' (-not $fs.Text.Contains('TieredStopAtLevel'))

Write-Host '--- a changed jar'
$deps = Join-Path $InstallDir 'bin\deps'
New-Item -ItemType Directory -Force -Path $deps | Out-Null
$someJar = Get-ChildItem (Join-Path $InstallDir 'bin\spark\jars\*.jar') | Select-Object -First 1
$addedJar = Join-Path $deps 'added-for-the-test.jar'
Copy-Item $someJar.FullName $addedJar
$disabled = Starlake 'validate' @{ SL_APPCDS = 'false' }
Check 'SL_APPCDS=false starts no rebuild' (-not $disabled.Text.Contains('rebuilding'))
$watch = [Diagnostics.Stopwatch]::StartNew()
$stale = Classes-From-Archive 'validate'
$watch.Stop()
Check 'an added jar starts a rebuild' ($stale.Text.Contains('rebuilding the AppCDS archive')) $stale.Text
Check 'a stale archive is not used' ($stale.Count -eq 0) "count=$($stale.Count)"
Check 'the command does not fail on a stale archive' ($stale.Code -eq 0) $stale.Text
Check 'the rebuild ends' (Wait-For-Rebuild)
if (Test-Path (Join-Path $appcds 'build.log')) { Write-Host (Get-Content -Raw (Join-Path $appcds 'build.log')) }
$rebuilt = Classes-From-Archive 'validate'
Write-Host "classes from the rebuilt archive: $($rebuilt.Count)"
Check 'a rebuilt archive replaced the previous one' ($rebuilt.Text -notmatch 'rebuilding') $rebuilt.Why
Check 'the next command uses the rebuilt archive' ($rebuilt.Count -gt 1000) "count=$($rebuilt.Count) $($rebuilt.Text)"
# A rebuild may still hold it
[void](Wait-For-Rebuild)
foreach ($try in 1..20) { Remove-Item $addedJar -ErrorAction SilentlyContinue; if (-not (Test-Path $addedJar)) { break }; Start-Sleep -Seconds 3 }
$removed = Starlake 'validate'
Check 'a removed jar makes the archive stale' ($removed.Text.Contains('rebuilding'))
Check 'the second rebuild ends' (Wait-For-Rebuild)

Write-Host '--- locks and failures'
New-Item -ItemType Directory -Path $lock | Out-Null
$busy = Starlake 'appcds-build'
Check 'a manual build does nothing while another build runs' ($busy.Text.Contains('already running')) $busy.Text
(Get-Item $lock).LastWriteTime = (Get-Date).AddMinutes(-20)
$reclaimed = Starlake 'appcds-build'
Check 'the lock of a build killed long ago is reclaimed' ($reclaimed.Text.Contains('Building the AppCDS archive')) $reclaimed.Text
Check 'a build releases its lock' (-not (Test-Path $lock))

$before = (Get-Item $archive).LastWriteTime
$jar = Get-ChildItem (Join-Path $InstallDir 'bin\spark\jars\spark-core*.jar') | Select-Object -First 1
Rename-Item $jar.FullName "$($jar.Name).off"
$failed = Starlake 'appcds-build' @{ SL_SKIP_CONSISTENCY_CHECK = '1' }
Rename-Item "$($jar.FullName).off" $jar.Name
Write-Host "exit code of a failed build: $($failed.Code)"
Check 'a failed build exits non-zero' ($failed.Code -ne 0) $failed.Text
Check 'a failed build records the failure' (Test-Path (Join-Path $appcds 'build.failed'))
Check 'a failed build keeps the previous archive' ((Get-Item $archive).LastWriteTime -eq $before)
Check 'a failed build releases its lock' (-not (Test-Path $lock))
$fresh = Classes-From-Archive 'validate'
Check 'the previous archive still works once the jars are back' ($fresh.Count -gt 1000) "count=$($fresh.Count) $($fresh.Text)"

Write-Host '--- JDK check'
$fakeJdk = Join-Path $env:TEMP 'sl-fake-jdk'
New-Item -ItemType Directory -Force -Path $fakeJdk | Out-Null
# From 1.9, Starflow requires Java 25; up to 1.8, the Hadoop bundled with Spark fails on JDK 24+.
$installedVersion = (Select-String -Path (Join-Path $InstallDir 'versions.cmd') -Pattern 'SL_VERSION=(\d+)\.(\d+)').Matches[0]
$requires25 = ([int]$installedVersion.Groups[1].Value -gt 1) -or ([int]$installedVersion.Groups[1].Value -eq 1 -and [int]$installedVersion.Groups[2].Value -ge 9)
$unsupportedJdk = if ($requires25) { '21' } else { '25' }
[IO.File]::WriteAllText((Join-Path $fakeJdk 'release'), "JAVA_VERSION=`"$unsupportedJdk.0.1`"`n")
$realJavaHome = $env:JAVA_HOME
$env:Path = "$realJavaHome\bin;$env:Path"
$refused = Starlake 'validate' @{ JAVA_HOME = $fakeJdk; SL_APPCDS = 'false' }
$env:JAVA_HOME = $realJavaHome
Check "an unsupported JDK ($unsupportedJdk) stops the command with an error that names the version" ($refused.Code -eq 1 -and $refused.Text.Contains("Error: Java $unsupportedJdk cannot run Starflow")) "code=$($refused.Code)`n$($refused.Text)"
Check 'the JDK error names the override' ($refused.Text.Contains('SL_SKIP_JDK_CHECK=1')) $refused.Text
$overridden = Starlake 'validate' @{ JAVA_HOME = $fakeJdk; SL_APPCDS = 'false'; SL_SKIP_JDK_CHECK = '1' }
$env:JAVA_HOME = $realJavaHome
Check 'SL_SKIP_JDK_CHECK turns the error into a warning, and the command runs' ($overridden.Code -eq 0 -and $overridden.Text.Contains("Warning: Java $unsupportedJdk cannot run Starflow") -and -not $overridden.Text.Contains('Error:')) "code=$($overridden.Code)`n$($overridden.Text)"
$supported = Starlake 'validate' @{ SL_APPCDS = 'false' }
Check 'a supported JDK prints no warning' (-not $supported.Text.Contains('cannot run Starflow'))
$env:JAVA_HOME = $realJavaHome

Write-Host '--- an install path with a space'
$spaced = Join-Path $env:TEMP 'sl with space'
New-Item -ItemType Directory -Force -Path $spaced | Out-Null
Copy-Item $launcher $spaced
Copy-Item (Join-Path $InstallDir 'versions.cmd') $spaced
cmd /c mklink /J "`"$spaced\bin`"" "`"$InstallDir\bin`"" | Out-Null
$skipped = Starlake 'appcds-build' @{} (Join-Path $spaced 'starlake.cmd')
Check 'an install path with a space is skipped' ($skipped.Text.Contains('Skipping the AppCDS archive')) $skipped.Text
Check 'an install path with a space builds nothing' (-not (Test-Path (Join-Path $spaced '.appcds\starlake.jsa')))
cmd /c rmdir "`"$spaced\bin`"" | Out-Null

if ($script:failures -gt 0) { Write-Host "$($script:failures) check(s) failed"; exit 1 }
Write-Host 'All checks passed'
exit 0
