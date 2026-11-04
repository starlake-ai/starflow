# The detection of a transform on a JDBC connection in distrib/starlake.cmd: the cases of
# starlake_appcds.bats. It runs the PowerShell block of the launcher as the launcher does, on any OS.
$ErrorActionPreference = 'Stop'
$launcher = (Resolve-Path (Join-Path $PSScriptRoot '../../../distrib/starlake.cmd')).Path
$shell = if (Get-Command powershell -ErrorAction SilentlyContinue) { 'powershell' } else { 'pwsh' }
$script:failures = 0
$overrides = 'SL_CONNECTION_REF', 'SL_ACTIVE_CONNECTION_REF', 'SL_TRANSFORM_CONNECTION_REF', 'SL_METADATA', 'SL_ENV', 'connectionRef'

$application = @'
---
version: 1
application:
  connectionRef: "{{connectionRef}}"
  audit:
    sink:
      connectionRef: "{{connectionRef}}"
  connections:
    sparkLocal:
      type: "fs"
    duckdb:
      type: "jdbc"
      options:
        url: "jdbc:duckdb:{{SL_ROOT}}/datasets/duckdb.db"
    bigquery:
      type: "bigquery"
'@

function Invoke-Case {
  param([string]$Name, [bool]$Expected, [hashtable]$Files = @{}, [hashtable]$Env = @{}, [string[]]$Remove = @(), [switch]$Empty)
  $root = Join-Path ([IO.Path]::GetTempPath()) ("sl-c1-" + [Guid]::NewGuid())
  $metadata = Join-Path $root 'metadata'
  New-Item -ItemType Directory -Force -Path (Join-Path $metadata 'transform/dom') | Out-Null
  if (-not $Empty) {
    $defaults = @{
      'application.sl.yml'     = $application
      'env.sl.yml'             = "version: 1`nenv:`n  connectionRef: `"duckdb`"`n"
      'transform/dom/t.sl.yml' = "version: 1`ntask:`n  name: `"t`"`n  domain: `"dom`"`n"
    }
    foreach ($file in $defaults.Keys) {
      if ($Remove -notcontains $file) { [IO.File]::WriteAllText((Join-Path $metadata $file), $defaults[$file]) }
    }
    foreach ($file in $Files.Keys) { [IO.File]::WriteAllText((Join-Path $metadata $file), $Files[$file]) }
  }
  foreach ($name2 in $overrides) { Remove-Item "Env:$name2" -ErrorAction SilentlyContinue }
  foreach ($key in $Env.Keys) { Set-Item "Env:$key" $Env[$key] }
  $env:SL_SELF = $launcher
  $env:SL_ROOT = $root
  $env:SL_PS_ACTION = 'c1'
  $command = '$s=[IO.File]::ReadAllText($env:SL_SELF);$m=''# SL_POWERSHELL_''+''BEGIN'';iex $s.Substring($s.IndexOf($m))'
  $output = & $shell -NoProfile -NonInteractive -Command $command 2>&1
  $actual = ($LASTEXITCODE -eq 0)
  foreach ($key in $Env.Keys) { Remove-Item "Env:$key" -ErrorAction SilentlyContinue }
  Remove-Item -Recurse -Force $root
  if ($actual -eq $Expected) { Write-Host "ok   $Name" }
  else { $script:failures++; Write-Host "FAIL $Name (expected $Expected, got $actual) $output" }
}

Invoke-Case 'a transform on a jdbc connection uses C1' $true
Invoke-Case 'an fs connection does not' $false -Files @{ 'env.sl.yml' = "env:`n  connectionRef: sparkLocal`n" }
Invoke-Case 'bigquery does not' $false -Files @{ 'env.sl.yml' = "env:`n  connectionRef: bigquery`n" }
Invoke-Case 'the env file of SL_ENV overrides the default env file' $true -Env @{ SL_ENV = 'DUCKDB' } -Files @{
  'env.sl.yml' = "env:`n  connectionRef: sparkLocal`n"; 'env.DUCKDB.sl.yml' = "env:`n  connectionRef: duckdb`n" }
Invoke-Case 'SL_ENV defined in the default env file selects the env file' $true -Files @{
  'env.sl.yml' = "env:`n  SL_ENV: DUCKDB`n  connectionRef: sparkLocal`n"; 'env.DUCKDB.sl.yml' = "env:`n  connectionRef: duckdb`n" }
Invoke-Case 'a literal connectionRef without env files works' $true -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`n  connectionRef: duckdb`n  connections:`n    duckdb:`n      type: jdbc`n" }
Invoke-Case 'transformConnectionRef wins over connectionRef' $true -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`n  connectionRef: sparkLocal`n  transformConnectionRef: duckdb`n  connections:`n    sparkLocal:`n      type: fs`n    duckdb:`n      type: jdbc`n" }
Invoke-Case 'a connectionRef nested before the top level one is not read' $false -Files @{
  'application.sl.yml' = "application:`n  audit:`n    sink:`n      connectionRef: duckdb`n  connectionRef: sparkLocal`n  connections:`n    sparkLocal:`n      type: fs`n    duckdb:`n      type: jdbc`n" }
Invoke-Case 'a connectionRef nested after the top level one is not read' $true -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`n  connectionRef: duckdb`n  audit:`n    sink:`n      connectionRef: sparkLocal`n  connections:`n    sparkLocal:`n      type: fs`n    duckdb:`n      type: jdbc`n" }
Invoke-Case 'only the type of the selected connection counts' $false -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`n  connectionRef: sparkLocal`n  connections:`n    duckdb:`n      type: jdbc`n    sparkLocal:`n      type: fs`n" }
Invoke-Case 'a jdbc connection read through Spark does not use C1' $false -Files @{
  'application.sl.yml' = $application.Replace('type: "jdbc"', "type: `"jdbc`"`n      sparkFormat: `"jdbc`"") }
Invoke-Case 'a connectionRef in the transforms disables C1' $false -Files @{
  'transform/dom/t.sl.yml' = "version: 1`ntask:`n  name: t`n  connectionRef: sparkLocal`n" }
Invoke-Case 'an unresolved connection disables C1' $false -Remove 'env.sl.yml'
Invoke-Case 'a project that cannot be read does not use C1' $false -Empty
foreach ($override in 'SL_CONNECTION_REF', 'SL_ACTIVE_CONNECTION_REF', 'SL_TRANSFORM_CONNECTION_REF', 'SL_METADATA') {
  Invoke-Case "$override disables C1" $false -Env @{ $override = 'x' }
}
Invoke-Case 'an environment variable overriding the template disables C1' $false -Env @{ connectionRef = 'sparkLocal' }
Invoke-Case 'quotes, single quotes, comments, CRLF and case are understood' $true -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`r`n  connectionRef: 'duck' # default`r`n  connections:`r`n    `"duck`":`r`n      type: `"JDBC`" # engine`r`n" }
Invoke-Case 'CRLF line endings without a trailing comment' $true -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`r`n  connectionRef: duck`r`n  connections:`r`n    duck:`r`n      type: jdbc`r`n" }
Invoke-Case 'a key named like the connection nested in another connection is not read' $false -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`n  connectionRef: target`n  connections:`n    other:`n      options:`n        target:`n          type: jdbc`n" }
Invoke-Case 'a key named like the connection outside the connections is not read' $false -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`n  connectionRef: target`n  target:`n    type: jdbc`n  connections:`n    other:`n      type: fs`n" }
Invoke-Case 'comments and blank lines inside a connection are skipped' $true -Remove 'env.sl.yml' -Files @{
  'application.sl.yml' = "application:`n  connectionRef: duck`n  connections:`n    duck:`n`n      # engine`n      type: jdbc`n" }

if ($script:failures -gt 0) { Write-Host "$($script:failures) case(s) failed"; exit 1 }
Write-Host 'All cases passed'
