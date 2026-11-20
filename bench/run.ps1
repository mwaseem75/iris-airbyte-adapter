# Usage: bench\run.ps1 <scenario>   (bench\run.ps1 list shows the names)
# The scenarios themselves are in bench.py, so run.sh and run.ps1 behave the same.
param([Parameter(Mandatory = $true)][string]$Scenario)
Set-Location (Split-Path -Parent $PSScriptRoot)
$py = $null
foreach ($c in "python3", "python") {
    $cmd = Get-Command $c -ErrorAction SilentlyContinue
    if ($cmd) { & $cmd.Source -c "import sys" 2>$null; if ($LASTEXITCODE -eq 0) { $py = $cmd.Source; break } }
}
if (-not $py) { Write-Error "Python 3 is needed on this machine to run the benchmark"; exit 1 }
& $py bench/bench.py $Scenario
exit $LASTEXITCODE
