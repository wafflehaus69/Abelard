# Deploy doctrine from the monorepo to OpenClaw's runtime workspace.
#
# On Windows-with-WSL, default target is the WSL workspace via the \\wsl$\ UNC path.
# Override via env var OPENCLAW_WORKSPACE.

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Src = Join-Path $RepoRoot "doctrine"
$Dst = if ($env:OPENCLAW_WORKSPACE) {
    $env:OPENCLAW_WORKSPACE
} else {
    "\\wsl`$\Ubuntu\home\wafflehouse\.openclaw\workspace"
}

if (-not (Test-Path $Src)) {
    Write-Error "doctrine directory not found at $Src"
    exit 1
}

if (-not (Test-Path $Dst)) {
    New-Item -ItemType Directory -Path $Dst -Force | Out-Null
}

$DoctrineFiles = @(
    "SOUL.md", "IDENTITY.md", "USER.md", "AGENTS.md", "SECURITY.md",
    "WORLDVIEW.md", "THESES.md", "METHODOLOGY.md", "MEMORY.md"
)

foreach ($f in $DoctrineFiles) {
    $SrcFile = Join-Path $Src $f
    $DstFile = Join-Path $Dst $f
    if (Test-Path $SrcFile) {
        Copy-Item $SrcFile -Destination $DstFile -Force
        Write-Host "Deployed: $f -> $DstFile"
    } else {
        Write-Warning "$f missing from $Src; skipping"
    }
}

Write-Host "Doctrine deployed to: $Dst"
