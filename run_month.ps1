# PowerShell wrapper for month-long rail delay data collection
# One-button runner for Windows

param(
    [string]$Config = "config/month_run.yaml"
)

Write-Host "🚂 Rail Delay Data Collection - Month Runner" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host ""

# Step 0: Health check
Write-Host "Step 0: Running health check..." -ForegroundColor Yellow
try {
    $healthResult = python routely-buffer-spike/healthcheck.py
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Health check failed!" -ForegroundColor Red
        if ($healthResult -match "403") {
            Write-Host "🚫 API returned 403 Forbidden. Check your API key or rate limits." -ForegroundColor Red
            Write-Host "   Aborting to prevent wasted API calls." -ForegroundColor Red
        }
        exit 1
    }
    Write-Host "✅ Health check passed" -ForegroundColor Green
} catch {
    Write-Host "❌ Health check script not found or failed to run" -ForegroundColor Red
    Write-Host "   Make sure routely-buffer-spike/healthcheck.py exists" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 1: Run orchestrator
Write-Host "Step 1: Starting month orchestration..." -ForegroundColor Yellow
Write-Host "Config: $Config" -ForegroundColor Cyan

try {
    python orchestrate_month.py --config $Config
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Orchestration failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "✅ Orchestration completed successfully" -ForegroundColor Green
} catch {
    Write-Host "❌ Failed to run orchestrator" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 2: Generate QA report
Write-Host "Step 2: Generating QA report..." -ForegroundColor Yellow
try {
    python qa_report.py --config $Config
    $qaExitCode = $LASTEXITCODE
    
    # Find the most recent QA report
    $reportDir = "reports"
    if (Test-Path $reportDir) {
        $latestReport = Get-ChildItem -Path $reportDir -Filter "qa_*.html" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($latestReport) {
            $reportPath = $latestReport.FullName
            Write-Host "📄 QA Report generated: $reportPath" -ForegroundColor Cyan
            Write-Host "   Open in browser: file:///$($reportPath.Replace('\', '/'))" -ForegroundColor Cyan
        }
    }
    
    if ($qaExitCode -ne 0) {
        Write-Host "❌ QA checks failed! Review the report above." -ForegroundColor Red
        exit $qaExitCode
    } else {
        Write-Host "✅ QA checks passed" -ForegroundColor Green
    }
} catch {
    Write-Host "⚠️  QA report generation failed, but data collection completed" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "🎉 Month data collection complete!" -ForegroundColor Green
Write-Host ""
Write-Host "📊 Output files:" -ForegroundColor Cyan
Write-Host "   • data/delays_raw.csv.gz (concatenated raw data)" -ForegroundColor White
Write-Host "   • data/leg_percentiles.csv (percentile analysis)" -ForegroundColor White
Write-Host "   • reports/qa_*.html (quality assurance report)" -ForegroundColor White
Write-Host "   • state/state.json (execution state for resume)" -ForegroundColor White
Write-Host ""
