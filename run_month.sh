#!/bin/bash
# Bash wrapper for month-long rail delay data collection
# One-button runner for Unix/Linux/macOS

set -e  # Stop on any non-zero exit code

CONFIG="${1:-config/month_run.yaml}"

echo "🚂 Rail Delay Data Collection - Month Runner"
echo "============================================="
echo ""

# Step 0: Health check
echo "Step 0: Running health check..."
if ! python routely-buffer-spike/healthcheck.py; then
    echo "❌ Health check failed!"
    # Check if the output contains 403
    if python routely-buffer-spike/healthcheck.py 2>&1 | grep -q "403"; then
        echo "🚫 API returned 403 Forbidden. Check your API key or rate limits."
        echo "   Aborting to prevent wasted API calls."
    fi
    exit 1
fi
echo "✅ Health check passed"
echo ""

# Step 1: Run orchestrator
echo "Step 1: Starting month orchestration..."
echo "Config: $CONFIG"
python orchestrate_month.py --config "$CONFIG"
echo "✅ Orchestration completed successfully"
echo ""

# Step 2: Generate QA report
echo "Step 2: Generating QA report..."
if python qa_report.py --config "$CONFIG"; then
    QA_EXIT_CODE=0
    echo "✅ QA checks passed"
else
    QA_EXIT_CODE=$?
    echo "❌ QA checks failed! Review the report below."
fi

# Find and display the most recent QA report
if [ -d "reports" ]; then
    LATEST_REPORT=$(find reports -name "qa_*.html" -type f -exec ls -t {} + | head -n1)
    if [ -n "$LATEST_REPORT" ]; then
        FULL_PATH=$(realpath "$LATEST_REPORT")
        echo "📄 QA Report generated: $FULL_PATH"
        echo "   Open in browser: file://$FULL_PATH"
    fi
fi

# Exit with QA result
if [ $QA_EXIT_CODE -ne 0 ]; then
    exit $QA_EXIT_CODE
fi

echo ""
echo "🎉 Month data collection complete!"
echo ""
echo "📊 Output files:"
echo "   • data/delays_raw.csv.gz (concatenated raw data)"
echo "   • data/leg_percentiles.csv (percentile analysis)" 
echo "   • reports/qa_*.html (quality assurance report)"
echo "   • state/state.json (execution state for resume)"
echo ""
