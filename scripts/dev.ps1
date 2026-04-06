# Local MySQL/Redis assumed up -> alembic -> new windows for API + Vite (no docker compose).
# Stops shells started by the previous run (same script) before opening new ones.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$PidFile = Join-Path $PSScriptRoot ".dev-shell-pids.json"

function Stop-PreviousDevShells {
    if (-not (Test-Path $PidFile)) {
        return
    }
    try {
        $saved = Get-Content -LiteralPath $PidFile -Raw -ErrorAction Stop | ConvertFrom-Json
    } catch {
        Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
        return
    }
    foreach ($prop in @("apiShellPid", "frontendShellPid")) {
        $raw = $saved.$prop
        if ($null -eq $raw) {
            continue
        }
        $targetPid = 0
        if (-not [int]::TryParse("$raw", [ref]$targetPid)) {
            continue
        }
        if ($targetPid -le 0) {
            continue
        }
        Write-Host "==> stop previous dev window (PID $targetPid)"
        # taskkill writes to stderr when PID is gone; avoid terminating under $ErrorActionPreference = Stop
        Start-Process -FilePath "taskkill.exe" -ArgumentList @("/PID", "$targetPid", "/T", "/F") -Wait -NoNewWindow | Out-Null
    }
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 600
}

Stop-PreviousDevShells

$shellExe = $null
$pwshCmd = Get-Command pwsh -ErrorAction SilentlyContinue
if ($pwshCmd) {
    $shellExe = $pwshCmd.Source
} else {
    $shellExe = (Get-Command powershell.exe -ErrorAction Stop).Source
}

Write-Host "==> skip docker compose (expect MySQL :3306, Redis :6379; match DATABASE_URL / REDIS_URL)"

$env:DATABASE_URL = "mysql+pymysql://root:root@127.0.0.1:3306/value_screener"
$env:REDIS_URL = "redis://127.0.0.1:6379/0"
$env:PYTHONPATH = "$Root\src"

Write-Host "==> alembic upgrade head (creates screening_run etc.)"
Start-Sleep -Seconds 2
python -m alembic upgrade head
if ($LASTEXITCODE -ne 0) {
    throw "Alembic failed (exit $LASTEXITCODE). Set DATABASE_URL and run from repo root: python -m alembic upgrade head"
}

Write-Host "==> API http://127.0.0.1:8000  frontend http://127.0.0.1:5173"
$importDotenv = Join-Path $PSScriptRoot "import-dotenv.ps1"
$pApi = Start-Process -FilePath $shellExe -PassThru -WindowStyle Normal -ArgumentList @(
    "-NoExit",
    "-Command",
    ". `"$importDotenv`"; Import-RepoDotEnv -RepoRoot `"$Root`"; Set-Location `"$Root`"; `$env:PYTHONPATH='$Root\src'; `$env:DATABASE_URL='$env:DATABASE_URL'; `$env:REDIS_URL='$env:REDIS_URL'; python -m uvicorn value_screener.interfaces.main:app --reload --host 0.0.0.0 --port 8000"
)
$pFe = Start-Process -FilePath $shellExe -PassThru -WindowStyle Normal -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$Root\frontend'; if (-not (Test-Path 'node_modules')) { npm install }; npm run dev"
)

@{ apiShellPid = $pApi.Id; frontendShellPid = $pFe.Id } | ConvertTo-Json | Set-Content -LiteralPath $PidFile -Encoding utf8

Write-Host "Done. Closed prior dev shells if any; opened two new terminal windows."
