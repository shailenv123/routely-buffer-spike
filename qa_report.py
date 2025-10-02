#!/usr/bin/env python3
"""
Automated QA report generator for rail delay data.
Generates HTML reports with coverage matrices, monotonicity checks, and hard gates.
"""

import os
import sys
import json
import pandas as pd
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import gzip


class QAReporter:
    """Generates comprehensive QA reports for rail delay data."""
    
    def __init__(self, config_path: str = "config/month_run.yaml"):
        self.config = self._load_config(config_path)
        self.qa_failures = []
        self.warnings = []
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _load_raw_data(self, data_path: str) -> pd.DataFrame:
        """Load and validate raw delay data."""
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Raw data file not found: {data_path}")
        
        df = pd.read_csv(data_path, compression='gzip')
        
        if df.empty:
            raise ValueError("Raw data file is empty")
        
        # Add derived columns
        df['date_dt'] = pd.to_datetime(df['date'])
        df['dow'] = df['date_dt'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['hour'] = df['gbtt_pta'].astype(str).str[:2].astype(int)
        
        return df
    
    def _load_percentiles_data(self, percentiles_path: str) -> pd.DataFrame:
        """Load percentiles data."""
        if not os.path.exists(percentiles_path):
            raise FileNotFoundError(f"Percentiles file not found: {percentiles_path}")
        
        return pd.read_csv(percentiles_path)
    
    def _load_slice_logs(self, logs_dir: str) -> List[Dict]:
        """Load all slice logs from the logs directory."""
        logs = []
        logs_path = Path(logs_dir)
        
        if not logs_path.exists():
            self.warnings.append(f"Slice logs directory not found: {logs_dir}")
            return logs
        
        for log_file in logs_path.glob("*.json"):
            try:
                with open(log_file, 'r') as f:
                    log_data = json.load(f)
                    logs.append(log_data)
            except Exception as e:
                self.warnings.append(f"Failed to read log file {log_file}: {e}")
        
        return logs
    
    def _generate_coverage_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate coverage matrix per route × hour × DOW."""
        # Group by route, hour, and day of week
        coverage = df.groupby(['origin', 'dest', 'hour', 'dow']).size().reset_index(name='obs_count')
        
        # Create a complete matrix with all possible combinations
        routes = [(origin, dest) for origin, dest in self.config['routes']]
        hours = range(24)
        dows = range(7)  # 0=Monday, 6=Sunday
        
        # Create complete index
        complete_index = []
        for origin, dest in routes:
            for hour in hours:
                for dow in dows:
                    complete_index.append({
                        'origin': origin,
                        'dest': dest, 
                        'hour': hour,
                        'dow': dow
                    })
        
        complete_df = pd.DataFrame(complete_index)
        
        # Merge with actual coverage data
        coverage_matrix = complete_df.merge(
            coverage, 
            on=['origin', 'dest', 'hour', 'dow'], 
            how='left'
        ).fillna(0)
        
        coverage_matrix['obs_count'] = coverage_matrix['obs_count'].astype(int)
        
        return coverage_matrix
    
    def _check_monotonicity(self, percentiles_df: pd.DataFrame) -> List[Dict]:
        """Check for monotonicity violations (p80 <= p90 <= p95)."""
        violations = []
        
        for _, row in percentiles_df.iterrows():
            p80, p90, p95 = row['p80'], row['p90'], row['p95']
            
            # Check monotonicity
            if p80 > p90 or p90 > p95:
                violations.append({
                    'origin': row['origin'],
                    'dest': row['dest'],
                    'hour': row['hour'],
                    'dow': row['dow'],
                    'p80': p80,
                    'p90': p90,
                    'p95': p95,
                    'violation_type': 'monotonicity'
                })
        
        return violations
    
    def _generate_sparkline_data(self, percentiles_df: pd.DataFrame) -> Dict:
        """Generate sparkline data for per-route p90 by hour."""
        sparklines = {}
        
        for origin, dest in self.config['routes']:
            route_key = f"{origin}_{dest}"
            route_data = percentiles_df[
                (percentiles_df['origin'] == origin) & 
                (percentiles_df['dest'] == dest)
            ]
            
            # Average p90 by hour across all days of week
            hourly_p90 = route_data.groupby('hour')['p90'].mean().reset_index()
            
            # Fill missing hours with 0
            all_hours = pd.DataFrame({'hour': range(24)})
            hourly_p90 = all_hours.merge(hourly_p90, on='hour', how='left').fillna(0)
            
            sparklines[route_key] = hourly_p90['p90'].tolist()
        
        return sparklines
    
    def _analyze_api_errors(self, slice_logs: List[Dict]) -> Dict:
        """Analyze API error counts from slice logs."""
        error_stats = {
            'total_slices': len(slice_logs),
            'successful_slices': 0,
            'failed_slices': 0,
            'timeout_errors': 0,
            '403_errors': 0,
            '4xx_errors': 0,
            '5xx_errors': 0,
            'other_errors': 0,
            'error_details': []
        }
        
        for log in slice_logs:
            result = log.get('result', {})
            status = result.get('status', 'unknown')
            error = result.get('error', '')
            
            if status == 'ok':
                error_stats['successful_slices'] += 1
            else:
                error_stats['failed_slices'] += 1
                
                # Categorize errors
                if 'timeout' in error.lower() or 'timed out' in error.lower():
                    error_stats['timeout_errors'] += 1
                elif '403' in error:
                    error_stats['403_errors'] += 1
                elif any(code in error for code in ['400', '401', '402', '404', '429']):
                    error_stats['4xx_errors'] += 1
                elif any(code in error for code in ['500', '501', '502', '503', '504']):
                    error_stats['5xx_errors'] += 1
                else:
                    error_stats['other_errors'] += 1
                
                # Store error details
                task = log.get('task', {})
                error_stats['error_details'].append({
                    'task_key': task.get('key', 'unknown'),
                    'error': error,
                    'status': status
                })
        
        return error_stats
    
    def _check_coverage_quality(self, coverage_matrix: pd.DataFrame) -> bool:
        """Check if coverage meets quality thresholds."""
        min_obs = self.config.get('qa_min_obs_per_cell', 8)
        
        # Calculate median observations per cell for each route
        route_medians = coverage_matrix.groupby(['origin', 'dest'])['obs_count'].median()
        
        quality_pass = True
        for (origin, dest), median_obs in route_medians.items():
            if median_obs < min_obs:
                self.qa_failures.append(
                    f"Route {origin}→{dest} has median {median_obs:.1f} obs/cell (< {min_obs} required)"
                )
                quality_pass = False
        
        return quality_pass
    
    def _generate_html_report(self, coverage_matrix: pd.DataFrame, monotonicity_violations: List[Dict], 
                            sparklines: Dict, error_stats: Dict, percentiles_df: pd.DataFrame) -> str:
        """Generate comprehensive HTML report."""
        
        # Day of week names
        dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Rail Delay QA Report - {timestamp}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .section {{ margin-bottom: 30px; }}
        .section h2 {{ color: #333; border-bottom: 2px solid #ddd; padding-bottom: 5px; }}
        .coverage-table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
        .coverage-table th, .coverage-table td {{ border: 1px solid #ddd; padding: 4px; text-align: center; }}
        .coverage-table th {{ background-color: #f5f5f5; }}
        .empty-cell {{ background-color: #ffcccc; }}
        .low-cell {{ background-color: #fff3cd; }}
        .good-cell {{ background-color: #d4edda; }}
        .violation {{ background-color: #f8d7da; padding: 10px; margin: 5px 0; border-radius: 3px; }}
        .error-summary {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; }}
        .sparkline {{ font-family: monospace; }}
        .qa-status {{ padding: 10px; border-radius: 5px; margin: 10px 0; }}
        .qa-pass {{ background-color: #d4edda; color: #155724; }}
        .qa-fail {{ background-color: #f8d7da; color: #721c24; }}
        .warning {{ background-color: #fff3cd; color: #856404; padding: 10px; border-radius: 5px; margin: 5px 0; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🚂 Rail Delay Data QA Report</h1>
        <p><strong>Generated:</strong> {timestamp}</p>
        <p><strong>Date Range:</strong> {self.config['date_start']} to {self.config['date_end']}</p>
        <p><strong>Routes:</strong> {len(self.config['routes'])}, <strong>Total Observations:</strong> {len(coverage_matrix[coverage_matrix['obs_count'] > 0]) if not coverage_matrix.empty and 'obs_count' in coverage_matrix.columns else 0}</p>
    </div>
"""
        
        # QA Status Summary
        qa_pass = len(self.qa_failures) == 0 and len(monotonicity_violations) == 0
        status_class = "qa-pass" if qa_pass else "qa-fail"
        status_text = "✅ PASS" if qa_pass else "❌ FAIL"
        
        html += f"""
    <div class="section">
        <h2>QA Status</h2>
        <div class="qa-status {status_class}">
            <strong>{status_text}</strong>
        </div>
"""
        
        # Show failures
        if self.qa_failures:
            html += "<h3>QA Failures:</h3>"
            for failure in self.qa_failures:
                html += f'<div class="violation">{failure}</div>'
        
        # Show warnings
        if self.warnings:
            html += "<h3>Warnings:</h3>"
            for warning in self.warnings:
                html += f'<div class="warning">{warning}</div>'
        
        html += "</div>"
        
        # Coverage Matrix
        html += """
    <div class="section">
        <h2>Coverage Matrix</h2>
        <p>Observations per route × hour × day-of-week. Empty cells highlighted in red, low coverage in yellow.</p>
"""
        
        min_obs = self.config.get('qa_min_obs_per_cell', 8)
        
        for origin, dest in self.config['routes']:
            if coverage_matrix.empty or 'origin' not in coverage_matrix.columns:
                html += f"<h3>{origin} → {dest}</h3>"
                html += "<p>No data available for this route.</p><br>"
                continue
                
            route_data = coverage_matrix[
                (coverage_matrix['origin'] == origin) & 
                (coverage_matrix['dest'] == dest)
            ]
            
            html += f"<h3>{origin} → {dest}</h3>"
            html += '<table class="coverage-table">'
            html += '<tr><th>Hour</th>'
            
            for dow in range(7):
                html += f'<th>{dow_names[dow]}</th>'
            html += '</tr>'
            
            for hour in range(24):
                html += f'<tr><td><strong>{hour:02d}</strong></td>'
                
                for dow in range(7):
                    obs_count = route_data[
                        (route_data['hour'] == hour) & 
                        (route_data['dow'] == dow)
                    ]['obs_count'].iloc[0] if len(route_data[
                        (route_data['hour'] == hour) & 
                        (route_data['dow'] == dow)
                    ]) > 0 else 0
                    
                    # Determine cell class
                    if obs_count == 0:
                        cell_class = "empty-cell"
                    elif obs_count < min_obs:
                        cell_class = "low-cell"
                    else:
                        cell_class = "good-cell"
                    
                    html += f'<td class="{cell_class}">{obs_count}</td>'
                
                html += '</tr>'
            
            html += '</table><br>'
        
        html += "</div>"
        
        # Monotonicity Violations
        html += f"""
    <div class="section">
        <h2>Monotonicity Check</h2>
        <p>Checking p80 ≤ p90 ≤ p95 constraint. Found {len(monotonicity_violations)} violations.</p>
"""
        
        if monotonicity_violations:
            html += '<table class="coverage-table">'
            html += '<tr><th>Route</th><th>Hour</th><th>DOW</th><th>p80</th><th>p90</th><th>p95</th></tr>'
            
            for violation in monotonicity_violations:
                dow_name = dow_names[violation['dow']]
                html += f"""
                <tr class="violation">
                    <td>{violation['origin']}→{violation['dest']}</td>
                    <td>{violation['hour']}</td>
                    <td>{dow_name}</td>
                    <td>{violation['p80']:.2f}</td>
                    <td>{violation['p90']:.2f}</td>
                    <td>{violation['p95']:.2f}</td>
                </tr>
                """
            
            html += '</table>'
        else:
            html += '<p style="color: green;">✅ No monotonicity violations found.</p>'
        
        html += "</div>"
        
        # Per-route P90 Sparklines
        html += """
    <div class="section">
        <h2>Per-Route P90 by Hour</h2>
        <p>Average P90 delay across all days, by hour of day.</p>
"""
        
        for origin, dest in self.config['routes']:
            route_key = f"{origin}_{dest}"
            if route_key in sparklines and sparklines[route_key]:
                values = sparklines[route_key]
                
                # Create simple text sparkline
                max_val = max(values) if max(values) > 0 else 1
                normalized = [int(v / max_val * 10) for v in values]
                sparkline_chars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']
                sparkline = ''.join(sparkline_chars[min(n, 7)] for n in normalized)
                
                html += f"""
                <div>
                    <strong>{origin} → {dest}:</strong> 
                    <span class="sparkline">{sparkline}</span>
                    (max: {max(values):.1f} min)
                </div>
                """
            else:
                html += f"""
                <div>
                    <strong>{origin} → {dest}:</strong> No data available
                </div>
                """
        
        html += "</div>"
        
        # API Error Analysis
        html += f"""
    <div class="section">
        <h2>API Error Analysis</h2>
        <div class="error-summary">
            <p><strong>Total Slices:</strong> {error_stats['total_slices']}</p>
            <p><strong>Successful:</strong> {error_stats['successful_slices']} ({error_stats['successful_slices']/max(error_stats['total_slices'], 1)*100:.1f}%)</p>
            <p><strong>Failed:</strong> {error_stats['failed_slices']}</p>
            <ul>
                <li>Timeout Errors: {error_stats['timeout_errors']}</li>
                <li>403 Errors: {error_stats['403_errors']}</li>
                <li>4xx Errors: {error_stats['4xx_errors']}</li>
                <li>5xx Errors: {error_stats['5xx_errors']}</li>
                <li>Other Errors: {error_stats['other_errors']}</li>
            </ul>
        </div>
"""
        
        # Show first 10 error details
        if error_stats['error_details']:
            html += "<h3>Error Details (first 10):</h3>"
            for error in error_stats['error_details'][:10]:
                html += f"""
                <div class="violation">
                    <strong>{error['task_key']}:</strong> {error['error'][:100]}...
                </div>
                """
        
        html += "</div>"
        
        # Summary Statistics
        total_obs = coverage_matrix['obs_count'].sum() if not coverage_matrix.empty and 'obs_count' in coverage_matrix.columns else 0
        empty_cells = (coverage_matrix['obs_count'] == 0).sum() if not coverage_matrix.empty and 'obs_count' in coverage_matrix.columns else 0
        total_cells = len(coverage_matrix) if not coverage_matrix.empty else 0
        
        html += f"""
    <div class="section">
        <h2>Summary Statistics</h2>
        <ul>
            <li><strong>Total Observations:</strong> {total_obs:,}</li>
            <li><strong>Total Cells:</strong> {total_cells:,} (route × hour × dow)</li>
            <li><strong>Empty Cells:</strong> {empty_cells:,} ({empty_cells/max(total_cells, 1)*100:.1f}%)</li>
            <li><strong>Average Obs per Cell:</strong> {total_obs/max(total_cells, 1):.1f}</li>
            <li><strong>Monotonicity Violations:</strong> {len(monotonicity_violations)}</li>
            <li><strong>QA Min Threshold:</strong> {self.config.get('qa_min_obs_per_cell', 8)} obs/cell</li>
        </ul>
    </div>

</body>
</html>
"""
        
        return html
    
    def generate_report(self, raw_data_path: str = "data/delays_raw.csv.gz", 
                       percentiles_path: str = "data/leg_percentiles.csv",
                       slice_logs_dir: str = "data/logs/slices") -> Tuple[str, bool]:
        """Generate QA report and return (report_path, qa_passed)."""
        
        print("🔍 Loading data for QA analysis...")
        
        # Load data
        try:
            raw_df = self._load_raw_data(raw_data_path)
            percentiles_df = self._load_percentiles_data(percentiles_path)
            slice_logs = self._load_slice_logs(slice_logs_dir)
        except Exception as e:
            self.qa_failures.append(f"Failed to load data: {e}")
            raw_df = pd.DataFrame()
            percentiles_df = pd.DataFrame()
            slice_logs = []
        
        print("📊 Generating coverage matrix...")
        coverage_matrix = self._generate_coverage_matrix(raw_df) if not raw_df.empty else pd.DataFrame()
        
        print("🔍 Checking monotonicity...")
        monotonicity_violations = self._check_monotonicity(percentiles_df) if not percentiles_df.empty else []
        
        print("📈 Generating sparklines...")
        sparklines = self._generate_sparkline_data(percentiles_df) if not percentiles_df.empty else {}
        
        print("⚠️ Analyzing API errors...")
        error_stats = self._analyze_api_errors(slice_logs)
        
        print("✅ Checking coverage quality...")
        coverage_quality = self._check_coverage_quality(coverage_matrix) if not coverage_matrix.empty else False
        
        # Add monotonicity failures to QA failures
        if monotonicity_violations:
            self.qa_failures.append(f"Found {len(monotonicity_violations)} monotonicity violations")
        
        # Generate HTML report
        print("📝 Generating HTML report...")
        html_content = self._generate_html_report(
            coverage_matrix, monotonicity_violations, sparklines, error_stats, percentiles_df
        )
        
        # Save report
        os.makedirs("reports", exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        report_path = f"reports/qa_{timestamp}.html"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"📄 Report saved: {report_path}")
        
        # Determine if QA passed
        qa_passed = len(self.qa_failures) == 0 and len(monotonicity_violations) == 0
        
        return report_path, qa_passed


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate QA report for rail delay data")
    parser.add_argument("--config", default="config/month_run.yaml", help="Config file path")
    parser.add_argument("--raw-data", default="data/delays_raw.csv.gz", help="Raw data file path")
    parser.add_argument("--percentiles", default="data/leg_percentiles.csv", help="Percentiles file path")
    parser.add_argument("--slice-logs", default="data/logs/slices", help="Slice logs directory")
    
    args = parser.parse_args()
    
    reporter = QAReporter(args.config)
    report_path, qa_passed = reporter.generate_report(
        args.raw_data, args.percentiles, args.slice_logs
    )
    
    # Print summary
    print("\n" + "=" * 60)
    print("🎯 QA REPORT SUMMARY")
    print("=" * 60)
    print(f"📄 Report: {report_path}")
    
    if qa_passed:
        print("✅ QA Status: PASS")
        print("🎉 All quality checks passed!")
        sys.exit(0)
    else:
        print("❌ QA Status: FAIL")
        print("\nFailures:")
        for failure in reporter.qa_failures:
            print(f"  • {failure}")
        sys.exit(1)


if __name__ == "__main__":
    main()
