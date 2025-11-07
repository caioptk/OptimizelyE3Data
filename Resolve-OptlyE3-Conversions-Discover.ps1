param(
  [string]$Root         = 'X:\v1\account_id=22397541806\type=events',
  [string]$StartDate    = '2024-10-30',
  [string]$EndDate      = '2025-10-29',
  [string]$PythonExe    = 'python',                  # uses your active .venv
  [string]$PyScript     = '.\load_optimizely_decisions_v2.py'  # relative to current v1 folder
)

if (-not $env:OPTIMIZELY_PAT) {
  Write-Error "Please set `$env:OPTIMIZELY_PAT (Optimizely Personal Access Token) before running."
  exit 1
}

Write-Host "== Dry-run to discover expected file counts per date =="
$log = Join-Path $env:TEMP ("e3_dryrun_{0}.log" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
& $PythonExe $PyScript `
  '--auth' 'optimizely' `
  '--type' 'events' `
  '--start-date' $StartDate `
  '--end-date'   $EndDate `
  '--dry-run' | Tee-Object -FilePath $log

if ($LASTEXITCODE -ne 0) {
  Write-Error "Dry-run failed. Check $log"
  exit 1
}

# Parse: lines like "... date=YYYY-MM-DD/ â€” N parquet file(s)"
$expected = @{}
Get-Content $log | ForEach-Object {
  $dm = [regex]::Match($_, 'date=(\d{4}-\d{2}-\d{2})/')
  $nm = [regex]::Match($_, '(\d+)\s+parquet file')
  if ($dm.Success -and $nm.Success) {
    $expected[$dm.Groups[1].Value] = [int]$nm.Groups[1].Value
  }
}

if ($expected.Keys.Count -eq 0) {
  Write-Error "Could not parse any per-date counts from the dry-run output. Inspect $log"
  exit 1
}

Write-Host "== Count local parquet files per date under $Root =="
$local = @{}
foreach ($d in $expected.Keys | Sort-Object) {
  $datePath = Join-Path $Root "date=$d"
  $count = 0
  if (Test-Path $datePath) {
    $count = (Get-ChildItem $datePath -Recurse -Filter '*.parquet' -ErrorAction SilentlyContinue | Measure-Object).Count
  }
  $local[$d] = $count
}

$complete   = @()
$incomplete = @()
foreach ($d in $expected.Keys | Sort-Object) {
  $exp = $expected[$d]; $loc = $local[$d]
  if     ($exp -gt 0 -and $loc -eq $exp) { $complete   += $d }
  elseif ($exp -gt 0 -and $loc -lt $exp) { $incomplete += $d }
}

Write-Host ""
Write-Host "=== Summary ==="
Write-Host ("Dates with FULL local coverage   : {0}" -f $complete.Count)
Write-Host ("Dates with PARTIAL/MISSING files : {0}" -f $incomplete.Count)
Write-Host ""
Write-Host "# Complete dates:";   Write-Host ($complete -join ', ')
Write-Host ""
Write-Host "# Incomplete dates:"; Write-Host ($incomplete -join ', ')

# Save handy CSVs for later steps
$csvDir = Join-Path $PWD "e3-reports"
New-Item -ItemType Directory -Force -Path $csvDir | Out-Null
$complete   | ForEach-Object { [PSCustomObject]@{date=$_} } | Export-Csv (Join-Path $csvDir "complete_dates.csv")   -NoTypeInformation -Encoding UTF8
$incomplete | ForEach-Object { [PSCustomObject]@{date=$_} } | Export-Csv (Join-Path $csvDir "incomplete_dates.csv") -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "Reports written to: $csvDir"
Write-Host " - complete_dates.csv"
Write-Host " - incomplete_dates.csv"
