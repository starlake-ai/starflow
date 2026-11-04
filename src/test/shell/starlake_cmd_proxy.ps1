# The proxy flags of distrib/starlake.cmd on Windows, with spark-submit stubbed: no network, no JVM.
$ErrorActionPreference = 'Continue'
$source = (Resolve-Path (Join-Path $PSScriptRoot '../../../distrib/starlake.cmd')).Path
$root = Join-Path $env:TEMP ('sl-proxy-' + [Guid]::NewGuid())
New-Item -ItemType Directory -Force -Path "$root\bin\sl", "$root\bin\deps", "$root\bin\spark\bin" | Out-Null
# With CRLF line endings, as setup.ps1 installs it
[IO.File]::WriteAllText("$root\starlake.cmd", ([IO.File]::ReadAllText($source) -replace "`r`n", "`n" -replace "`n", "`r`n"))
[IO.File]::WriteAllText("$root\versions.cmd", "set SL_VERSION=0.0.0`r`nset SPARK_VERSION=4.1.3`r`nset HADOOP_VERSION=3`r`n")
[IO.File]::WriteAllText("$root\bin\sl\starlake-core_2.13-0.0.0-assembly.jar", '')
[IO.File]::WriteAllText("$root\bin\spark\bin\spark-submit.cmd", "@echo SUBMIT %*`r`n")
$env:SL_SKIP_CONSISTENCY_CHECK = '1'
$script:failures = 0

function Invoke-Launcher([hashtable]$environment) {
  foreach ($key in 'HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy') { Remove-Item "Env:$key" -ErrorAction SilentlyContinue }
  foreach ($key in $environment.Keys) { Set-Item "Env:$key" $environment[$key] }
  $output = (& cmd /c "`"$root\starlake.cmd`" validate 2>&1" | ForEach-Object { "$_" }) -join "`n"
  foreach ($key in $environment.Keys) { Remove-Item "Env:$key" -ErrorAction SilentlyContinue }
  return $output
}

function Check-Proxy($name, [hashtable]$environment, [string[]]$expected) {
  $output = Invoke-Launcher $environment
  $problems = @()
  if ($output -notmatch 'SUBMIT') { $problems += 'spark-submit was not reached' }
  foreach ($flag in $expected) { if (-not $output.Contains($flag)) { $problems += "no $flag" } }
  if ($problems.Count -eq 0) { Write-Host "ok   $name" }
  else { $script:failures++; Write-Host "FAIL $name ($($problems -join ', '))`n$output" }
}

Check-Proxy 'no proxy' @{} @()
Check-Proxy 'HTTP_PROXY' @{ HTTP_PROXY = 'http://proxy.example:3128' } @('-Dhttp.proxyHost=proxy.example', '-Dhttp.proxyPort=3128')
Check-Proxy 'HTTPS_PROXY' @{ HTTPS_PROXY = 'http://proxy.example:3129' } @('-Dhttps.proxyHost=proxy.example', '-Dhttps.proxyPort=3129')
Check-Proxy 'both' @{ HTTP_PROXY = 'http://one.example:1'; HTTPS_PROXY = 'http://two.example:2' } @('-Dhttp.proxyHost=one.example', '-Dhttps.proxyHost=two.example')
Check-Proxy 'no protocol' @{ HTTP_PROXY = 'proxy.example:3128' } @('-Dhttp.proxyHost=proxy.example')
Check-Proxy 'credentials' @{ HTTP_PROXY = 'http://me:secret@proxy.example:3128' } @('-Dhttp.proxyUser=me', '-Dhttp.proxyPassword=secret')
Check-Proxy 'a URL encoded password' @{ HTTP_PROXY = 'http://me:p%40ss@proxy.example:3128' } @('-Dhttp.proxyPassword=p@ss')
Check-Proxy 'a password with a special character of cmd' @{ HTTP_PROXY = 'http://me:a&b@proxy.example:3128' } @('-Dhttp.proxyHost=proxy.example')

Remove-Item -Recurse -Force $root
if ($script:failures -gt 0) { Write-Host "$($script:failures) case(s) failed"; exit 1 }
Write-Host 'All cases passed'
