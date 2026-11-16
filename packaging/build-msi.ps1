param(
    [switch]$SkipCargoBuild
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$OutDir = Join-Path $PSScriptRoot "out"
$StageDir = Join-Path $OutDir "dist"

if (-not $SkipCargoBuild) {
    Write-Host "Building release binaries (helixgrid.exe & helixgrid-worker.exe)..."
    Push-Location $RepoRoot
    try {
        cargo build --release -p helixgrid-worker -p helixgrid-studio
        if ($LASTEXITCODE -ne 0) {
            throw "cargo build --release failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
}

New-Item -ItemType Directory -Force -Path $StageDir | Out-Null
$ReleaseDir = Join-Path $RepoRoot "target\release"
Copy-Item -Force (Join-Path $ReleaseDir "helixgrid.exe") (Join-Path $StageDir "helixgrid.exe")
Copy-Item -Force (Join-Path $ReleaseDir "helixgrid-worker.exe") (Join-Path $StageDir "helixgrid-worker.exe")

$MsiPath = Join-Path $OutDir "helixgrid-0.1.0-x64.msi"
$WxsPath = Join-Path $PSScriptRoot "helixgrid.wxs"

Write-Host "Building WiX v5 MSI installer at $MsiPath..."
wix build $WxsPath -arch x64 -d "DistDir=$StageDir" -d "RepoRoot=$RepoRoot" -o $MsiPath
if ($LASTEXITCODE -ne 0) {
    throw "wix build failed with exit code $LASTEXITCODE"
}

Write-Host "Successfully built installer: $MsiPath"
Get-Item $MsiPath | Select-Object Name, Length, FullName
