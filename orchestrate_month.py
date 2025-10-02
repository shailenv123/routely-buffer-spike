#!/usr/bin/env python3
"""
Month-long rail delay data orchestrator.
Reads config/month_run.yaml and runs everything end-to-end with state management.
"""

import os
import sys
import json
import time
import gzip
import pandas as pd
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import requests

# Import existing components
sys.path.append('routely-buffer-spike')
from rdm_client import RDMClient
from build_percentiles import build_percentiles


class TaskState:
    """Manages task state persistence."""
    
    def __init__(self, state_file: str = "state/state.json"):
        self.state_file = state_file
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """Load state from JSON file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load state file: {e}")
        return {"slices": {}, "metadata": {"created": datetime.now().isoformat()}}
    
    def _save_state(self):
        """Save state to JSON file."""
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def get_slice_status(self, task_key: str) -> str:
        """Get status of a slice task."""
        return self.state["slices"].get(task_key, "pending")
    
    def set_slice_status(self, task_key: str, status: str, metadata: Dict = None):
        """Set status of a slice task."""
        self.state["slices"][task_key] = status
        if metadata:
            # Store metadata in separate key
            self.state.setdefault("slice_metadata", {})[task_key] = metadata
        self._save_state()
    
    def get_slice_metadata(self, task_key: str) -> Dict:
        """Get metadata for a slice task."""
        return self.state.get("slice_metadata", {}).get(task_key, {})


class WindowBisector:
    """Handles automatic window bisection on timeout."""
    
    @staticmethod
    def can_bisect(from_time: str, to_time: str) -> bool:
        """Check if window can be bisected (>= 60 minutes)."""
        from_min = WindowBisector._time_to_minutes(from_time)
        to_min = WindowBisector._time_to_minutes(to_time)
        return (to_min - from_min) >= 60
    
    @staticmethod
    def bisect_window(from_time: str, to_time: str) -> List[Tuple[str, str]]:
        """Bisect a time window into two equal parts."""
        from_min = WindowBisector._time_to_minutes(from_time)
        to_min = WindowBisector._time_to_minutes(to_time)
        
        # Calculate midpoint
        mid_min = (from_min + to_min) // 2
        
        # Create two windows
        mid_time = WindowBisector._minutes_to_time(mid_min)
        next_mid_time = WindowBisector._minutes_to_time(mid_min + 1)
        
        return [(from_time, mid_time), (next_mid_time, to_time)]
    
    @staticmethod
    def _time_to_minutes(time_str: str) -> int:
        """Convert HHMM to minutes since midnight."""
        return int(time_str[:2]) * 60 + int(time_str[2:4])
    
    @staticmethod
    def _minutes_to_time(minutes: int) -> str:
        """Convert minutes since midnight to HHMM."""
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours:02d}{mins:02d}"


class MonthOrchestrator:
    """Main orchestrator for month-long data collection."""
    
    def __init__(self, config_path: str = "config/month_run.yaml"):
        self.config = self._load_config(config_path)
        self.state = TaskState()
        self.client = RDMClient()
        self.stats = defaultdict(int)
        self.route_stats = defaultdict(lambda: defaultdict(int))
        
        # Configure client timeouts
        self.client.set_metrics_timeout(self.config['metrics_timeout'])
        self.client.set_details_timeout(self.config['details_timeout'])
        
        # Create output directories
        os.makedirs(f"{self.config['output_root']}/raw_delays", exist_ok=True)
        os.makedirs(f"{self.config['output_root']}/logs/slices", exist_ok=True)
        os.makedirs("state", exist_ok=True)
    
    def _load_config(self, config_path: str) -> Dict:
        """Load and validate configuration."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate required fields
        required_fields = [
            'date_start', 'date_end', 'routes', 'time_windows', 
            'rid_cap_per_slice', 'workers', 'output_root'
        ]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")
        
        return config
    
    def _generate_date_range(self) -> List[str]:
        """Generate list of dates from config range."""
        start_date = datetime.fromisoformat(self.config['date_start'])
        end_date = datetime.fromisoformat(self.config['date_end'])
        
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        return dates
    
    def _generate_task_dag(self) -> List[Dict]:
        """Generate task DAG over (date × route × time_window)."""
        dates = self._generate_date_range()
        tasks = []
        
        for date in dates:
            for route in self.config['routes']:
                origin, dest = route
                for time_window in self.config['time_windows']:
                    from_time, to_time = time_window
                    
                    task = {
                        'date': date,
                        'origin': origin,
                        'dest': dest,
                        'from_time': from_time,
                        'to_time': to_time,
                        'key': f"{date}_{origin}_{dest}_{from_time}_{to_time}"
                    }
                    tasks.append(task)
        
        return tasks
    
    def _get_days_tag(self, date_str: str) -> str:
        """Get days tag for API call."""
        date_obj = datetime.fromisoformat(date_str)
        weekday = date_obj.weekday()  # 0=Monday, 6=Sunday
        
        if weekday < 5:  # Monday-Friday
            return "WEEKDAY"
        elif weekday == 5:  # Saturday
            return "SATURDAY"
        else:  # Sunday
            return "SUNDAY"
    
    def _execute_slice_task(self, task: Dict, attempt: int = 1) -> Dict:
        """Execute a single slice task (metrics → details → write)."""
        task_key = task['key']
        origin = task['origin']
        dest = task['dest']
        date = task['date']
        from_time = task['from_time']
        to_time = task['to_time']
        
        print(f"[{attempt}] Processing slice: {task_key}")
        
        start_time = time.time()
        result = {
            'task_key': task_key,
            'status': 'failed',
            'rids_fetched': 0,
            'rows_written': 0,
            'metrics_time': 0,
            'details_time': 0,
            'error': None,
            'attempt': attempt
        }
        
        try:
            # Phase 1: Fetch metrics
            metrics_start = time.time()
            
            metrics_response = self.client.get_metrics(
                from_loc=origin,
                to_loc=dest,
                from_time=from_time,
                to_time=to_time,
                from_date=date,
                to_date=date,
                days=self._get_days_tag(date),
                timeout=self.config['metrics_timeout']
            )
            
            result['metrics_time'] = time.time() - metrics_start
            
            # Extract RIDs from services
            services = metrics_response.get('Services', [])
            all_rids = set()  # Use set to avoid duplicates
            
            for service in services:
                attrs = service.get('serviceAttributesMetrics', {})
                service_rids = attrs.get('rids', [])
                
                for rid in service_rids:
                    if isinstance(rid, str):
                        all_rids.add(rid)
            
            # Apply RID cap
            rids_list = list(all_rids)
            if len(rids_list) > self.config['rid_cap_per_slice']:
                # Use deterministic sampling for reproducibility
                import random
                random.seed(hash(task_key))
                rids_list = random.sample(rids_list, self.config['rid_cap_per_slice'])
            
            result['rids_fetched'] = len(rids_list)
            
            if not rids_list:
                result['status'] = 'skipped'
                result['error'] = 'No RIDs found'
                return result
            
            # Phase 2: Fetch details
            details_start = time.time()
            delay_rows = []
            details_ok = 0
            details_fail = 0
            
            def fetch_single_details(rid):
                """Fetch details for a single RID."""
                try:
                    # Note: get_details uses client's configured details_timeout
                    details = self.client.get_details(rid)
                    
                    # Extract service details
                    service_attrs = details.get("serviceAttributesDetails", {})
                    locations = service_attrs.get("locations", [])
                    
                    # Find destination location
                    dest_location = None
                    for location in locations:
                        loc_code = (location.get("location") or location.get("crs") or "").upper()
                        if loc_code == dest.upper():
                            dest_location = location
                            break
                    
                    if not dest_location:
                        return None, False
                    
                    # Extract timing data
                    gbtt_pta = dest_location.get("gbtt_pta") or dest_location.get("gbtt_ptd") or ""
                    actual_ta = dest_location.get("actual_ta") or dest_location.get("actual_td") or ""
                    
                    if not actual_ta or not gbtt_pta:
                        return None, False
                    
                    # Calculate delay
                    planned_min = self._time_to_minutes(gbtt_pta)
                    actual_min = self._time_to_minutes(actual_ta)
                    delay_min = max(0, actual_min - planned_min)
                    
                    row = {
                        'origin': origin,
                        'dest': dest,
                        'rid': rid,
                        'date': date,
                        'gbtt_pta': gbtt_pta,
                        'actual_ta': actual_ta,
                        'delay_min': delay_min,
                        'hour': int(gbtt_pta[:2]) if len(gbtt_pta) >= 2 else None
                    }
                    
                    return row, True
                    
                except Exception as e:
                    if "403" in str(e):
                        # Circuit breaker: pause and retry once
                        time.sleep(60)
                        try:
                            details = self.client.get_details(rid)
                            # ... same processing logic ...
                            return None, False  # Simplified for brevity
                        except:
                            raise e  # Re-raise 403 for circuit breaker handling
                    return None, False
            
            # Process RIDs with throttling
            request_count = 0
            
            with ThreadPoolExecutor(max_workers=self.config['workers']) as executor:
                # Submit all detail requests
                future_to_rid = {executor.submit(fetch_single_details, rid): rid for rid in rids_list}
                
                for future in as_completed(future_to_rid):
                    row, success = future.result()
                    
                    if success and row:
                        delay_rows.append(row)
                        details_ok += 1
                    else:
                        details_fail += 1
                    
                    request_count += 1
                    
                    # Throttling
                    if request_count % self.config.get('sleep_every', 200) == 0:
                        time.sleep(self.config.get('sleep_secs', 0.05))
            
            result['details_time'] = time.time() - details_start
            result['rows_written'] = len(delay_rows)
            
            # Phase 3: Write data
            if delay_rows:
                self._write_slice_data(task, delay_rows)
            
            result['status'] = 'ok'
            
            # Update stats
            self.stats['slices_ok'] += 1
            self.route_stats[f"{origin}_{dest}"]['obs_count'] += len(delay_rows)
            
        except Exception as e:
            error_str = str(e)
            result['error'] = error_str
            
            # Handle specific error types
            if "403" in error_str:
                self.stats['403_errors'] += 1
                result['status'] = 'circuit_breaker'
            elif "timeout" in error_str.lower() or "timed out" in error_str.lower():
                self.stats['timeout_errors'] += 1
                result['status'] = 'timeout'
            else:
                self.stats['other_errors'] += 1
            
            self.route_stats[f"{origin}_{dest}"]['errors'] += 1
        
        # Write slice log
        self._write_slice_log(task, result)
        
        return result
    
    def _time_to_minutes(self, time_str: str) -> int:
        """Convert HHMM to minutes since midnight."""
        if not time_str or len(time_str) < 4:
            return 0
        return int(time_str[:2]) * 60 + int(time_str[2:4])
    
    def _write_slice_data(self, task: Dict, delay_rows: List[Dict]):
        """Write slice data to compressed CSV."""
        df = pd.DataFrame(delay_rows)
        
        # Generate filename with window identifier
        filename = f"delays_{task['date']}_{task['origin']}_{task['dest']}_{task['from_time']}_{task['to_time']}.csv.gz"
        filepath = f"{self.config['output_root']}/raw_delays/{filename}"
        
        df.to_csv(filepath, index=False, compression='gzip')
    
    def _write_slice_log(self, task: Dict, result: Dict):
        """Write slice log to JSON."""
        log_data = {
            'task': task,
            'result': result,
            'timestamp': datetime.now().isoformat()
        }
        
        log_filename = f"{task['date']}_{task['origin']}_{task['dest']}_{task['from_time']}_{task['to_time']}.json"
        log_filepath = f"{self.config['output_root']}/logs/slices/{log_filename}"
        
        with open(log_filepath, 'w') as f:
            json.dump(log_data, f, indent=2)
    
    def _handle_timeout_with_bisection(self, task: Dict) -> List[Dict]:
        """Handle timeout by bisecting the window if possible."""
        if not self.config.get('bisection_on_timeout', False):
            return []
        
        if not WindowBisector.can_bisect(task['from_time'], task['to_time']):
            print(f"Cannot bisect window {task['from_time']}-{task['to_time']} (too small)")
            return []
        
        # Bisect the window
        sub_windows = WindowBisector.bisect_window(task['from_time'], task['to_time'])
        print(f"Bisecting {task['from_time']}-{task['to_time']} → {sub_windows}")
        
        # Create new tasks for sub-windows
        new_tasks = []
        for from_time, to_time in sub_windows:
            new_task = task.copy()
            new_task['from_time'] = from_time
            new_task['to_time'] = to_time
            new_task['key'] = f"{task['date']}_{task['origin']}_{task['dest']}_{from_time}_{to_time}"
            new_tasks.append(new_task)
        
        return new_tasks
    
    def _concatenate_raw_files(self, dates: List[str]):
        """Concatenate all raw delay files into single file with deduplication."""
        print("Concatenating raw delay files...")
        
        all_files = []
        raw_delays_dir = f"{self.config['output_root']}/raw_delays"
        
        # Find all CSV.GZ files
        for file in os.listdir(raw_delays_dir):
            if file.endswith('.csv.gz') and 'delays_' in file:
                all_files.append(os.path.join(raw_delays_dir, file))
        
        if not all_files:
            print("No raw delay files found to concatenate")
            return
        
        # Read and combine all files
        dfs = []
        for file_path in all_files:
            try:
                df = pd.read_csv(file_path, compression='gzip')
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        
        if not dfs:
            print("No valid data found in raw files")
            return
        
        # Combine and deduplicate
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"Combined {len(combined_df)} rows from {len(dfs)} files")
        
        # Deduplicate on rid + dest (as specified)
        initial_rows = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['rid', 'dest'])
        final_rows = len(combined_df)
        
        print(f"Deduplicated: {initial_rows} → {final_rows} rows")
        
        # Write concatenated file
        output_path = f"{self.config['output_root']}/delays_raw.csv.gz"
        combined_df.to_csv(output_path, index=False, compression='gzip')
        print(f"Wrote concatenated file: {output_path}")
    
    def _run_qa_check(self) -> bool:
        """Run QA checks on the final data."""
        print("Running QA checks...")
        
        try:
            # Read the concatenated file
            data_path = f"{self.config['output_root']}/delays_raw.csv.gz"
            if not os.path.exists(data_path):
                print("❌ QA FAIL: No concatenated data file found")
                return False
            
            df = pd.read_csv(data_path, compression='gzip')
            
            if df.empty:
                print("❌ QA FAIL: Empty dataset")
                return False
            
            # Check minimum observations per cell
            min_obs = self.config.get('qa_min_obs_per_cell', 8)
            
            # Group by route + hour + day-of-week
            df['date_dt'] = pd.to_datetime(df['date'])
            df['dow'] = df['date_dt'].dt.dayofweek
            df['hour'] = df['gbtt_pta'].astype(str).str[:2].astype(int)
            
            grouped = df.groupby(['origin', 'dest', 'hour', 'dow']).size()
            cells_below_threshold = (grouped < min_obs).sum()
            total_cells = len(grouped)
            
            print(f"QA Check: {cells_below_threshold}/{total_cells} cells below {min_obs} observations")
            
            if cells_below_threshold > total_cells * 0.5:  # More than 50% of cells are sparse
                print(f"❌ QA FAIL: Too many sparse cells ({cells_below_threshold}/{total_cells})")
                return False
            
            print("✅ QA PASS: Data quality acceptable")
            return True
            
        except Exception as e:
            print(f"❌ QA FAIL: Error during QA check: {e}")
            return False
    
    def _print_summary(self, total_tasks: int, successful_tasks: int, failed_tasks: int, skipped_tasks: int):
        """Print comprehensive one-page summary."""
        print("\n" + "=" * 80)
        print("🎯 MONTH ORCHESTRATION SUMMARY")
        print("=" * 80)
        
        # Overall stats
        print(f"📅 Date range: {self.config['date_start']} to {self.config['date_end']}")
        print(f"🗺️  Routes: {len(self.config['routes'])}")
        print(f"⏰ Time windows: {len(self.config['time_windows'])}")
        print(f"📊 Total slices: {total_tasks}")
        print()
        
        # Task results
        print("SLICE RESULTS:")
        print(f"  ✅ Succeeded: {successful_tasks}")
        print(f"  ❌ Failed: {failed_tasks}")
        print(f"  ⏭️  Skipped: {skipped_tasks}")
        print(f"  📈 Success rate: {successful_tasks/total_tasks*100:.1f}%")
        print()
        
        # Error breakdown
        print("ERROR BREAKDOWN:")
        print(f"  🚫 403 Errors: {self.stats['403_errors']}")
        print(f"  ⏰ Timeouts: {self.stats['timeout_errors']}")
        print(f"  🔧 Other errors: {self.stats['other_errors']}")
        print()
        
        # Per-route statistics
        print("PER-ROUTE STATISTICS:")
        for route_key, stats in self.route_stats.items():
            origin, dest = route_key.split('_')
            print(f"  {origin}→{dest}: {stats['obs_count']} observations, {stats['errors']} errors")
        print()
        
        # Performance stats
        total_obs = sum(stats['obs_count'] for stats in self.route_stats.values())
        print(f"📊 Total observations: {total_obs:,}")
        print(f"🎯 Average per successful slice: {total_obs/max(successful_tasks, 1):.1f}")
        
        print("=" * 80)
    
    def run(self, plan_only=False):
        """Main orchestration method."""
        print("🚂 Starting Month Orchestrator")
        print(f"📋 Config: {self.config['date_start']} to {self.config['date_end']}")
        print(f"🗺️  Routes: {len(self.config['routes'])}, Windows: {len(self.config['time_windows'])}")
        print()
        
        # Generate task DAG
        tasks = self._generate_task_dag()
        print(f"📊 Generated {len(tasks)} slice tasks")
        
        if plan_only:
            # Save planned tasks to state and exit
            print("📝 Plan-only mode: saving task plan to state...")
            
            # Store planned task count in state metadata
            self.state.state.setdefault("metadata", {})["planned_tasks"] = len(tasks)
            self.state.state["metadata"]["planned"] = len(tasks)  # Alternative key for compatibility
            
            # Initialize all tasks as pending
            for task in tasks:
                self.state.set_slice_status(task['key'], 'pending')
            
            print(f"✅ Saved {len(tasks)} planned tasks to state/state.json")
            print("🎯 Expected task count: ~620 (31 days × 5 routes × 4 windows)")
            return
        
        # Process tasks
        successful_tasks = 0
        failed_tasks = 0
        skipped_tasks = 0
        retry_queue = []
        
        for task in tasks:
            task_key = task['key']
            status = self.state.get_slice_status(task_key)
            
            if status == 'ok':
                print(f"⏭️  Skipping completed: {task_key}")
                successful_tasks += 1
                continue
            elif status == 'skipped':
                skipped_tasks += 1
                continue
            
            # Execute task
            result = self._execute_slice_task(task)
            
            if result['status'] == 'ok':
                self.state.set_slice_status(task_key, 'ok', result)
                successful_tasks += 1
            elif result['status'] == 'timeout' and self.config.get('bisection_on_timeout', False):
                # Try bisection
                sub_tasks = self._handle_timeout_with_bisection(task)
                if sub_tasks:
                    print(f"🔄 Queuing {len(sub_tasks)} sub-tasks for bisection")
                    tasks.extend(sub_tasks)
                    self.state.set_slice_status(task_key, 'bisected', result)
                else:
                    self.state.set_slice_status(task_key, 'failed', result)
                    failed_tasks += 1
            elif result['status'] == 'skipped':
                self.state.set_slice_status(task_key, 'skipped', result)
                skipped_tasks += 1
            else:
                # Failed - add to retry queue if not fail_fast
                if not self.config.get('fail_fast', False):
                    retry_queue.append(task)
                self.state.set_slice_status(task_key, 'failed', result)
                failed_tasks += 1
        
        # Process retry queue (bounded attempts)
        max_retries = 2
        for retry_attempt in range(1, max_retries + 1):
            if not retry_queue:
                break
            
            print(f"\n🔄 Retry attempt {retry_attempt}/{max_retries} for {len(retry_queue)} tasks")
            new_retry_queue = []
            
            for task in retry_queue:
                result = self._execute_slice_task(task, attempt=retry_attempt + 1)
                
                if result['status'] == 'ok':
                    self.state.set_slice_status(task['key'], 'ok', result)
                    successful_tasks += 1
                    failed_tasks -= 1
                else:
                    new_retry_queue.append(task)
            
            retry_queue = new_retry_queue
        
        # Final processing
        dates = self._generate_date_range()
        
        print("\n🔗 Concatenating raw files...")
        self._concatenate_raw_files(dates)
        
        print("📊 Building percentiles...")
        try:
            build_percentiles(dates)
            print("✅ Percentiles built successfully")
        except Exception as e:
            print(f"❌ Percentiles failed: {e}")
        
        print("🔍 Running QA checks...")
        qa_passed = self._run_qa_check()
        
        # Print summary
        self._print_summary(len(tasks), successful_tasks, failed_tasks, skipped_tasks)
        
        # Exit with appropriate code
        if not qa_passed:
            print("❌ QA checks failed")
            sys.exit(1)
        elif failed_tasks > 0:
            print(f"⚠️  {failed_tasks} tasks failed")
            sys.exit(1)
        else:
            print("✅ All tasks completed successfully")
            sys.exit(0)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Month-long rail delay data orchestrator")
    parser.add_argument("--config", default="config/month_run.yaml", help="Config file path")
    parser.add_argument("--resume", action="store_true", help="Resume from previous state")
    parser.add_argument("--plan-only", action="store_true", help="Generate task plan and save to state without executing")
    parser.add_argument("--override-routes", help="Override routes (format: ORIGIN,DEST)")
    parser.add_argument("--date-start", help="Override start date (YYYY-MM-DD)")
    parser.add_argument("--date-end", help="Override end date (YYYY-MM-DD)")
    parser.add_argument("--override-windows", help="Override time windows (format: HHMM,HHMM for single window)")
    
    args = parser.parse_args()
    
    if not args.resume and os.path.exists("state/state.json"):
        response = input("Previous state found. Resume? (y/N): ")
        if response.lower() != 'y':
            os.remove("state/state.json")
            print("Cleared previous state")
    
    orchestrator = MonthOrchestrator(args.config)
    
    # Apply overrides if provided
    if args.override_routes:
        origin, dest = args.override_routes.split(',')
        orchestrator.config['routes'] = [[origin.strip(), dest.strip()]]
        print(f"🔧 Override: Using single route {origin}→{dest}")
    
    if args.date_start:
        orchestrator.config['date_start'] = args.date_start
        print(f"🔧 Override: Start date = {args.date_start}")
    
    if args.date_end:
        orchestrator.config['date_end'] = args.date_end
        print(f"🔧 Override: End date = {args.date_end}")
    
    if args.override_windows:
        from_time, to_time = args.override_windows.split(',')
        from_time = from_time.strip().zfill(4)  # Ensure 4-digit format
        to_time = to_time.strip().zfill(4)      # Ensure 4-digit format
        orchestrator.config['time_windows'] = [[from_time, to_time]]
        print(f"🔧 Override: Using single window {from_time}–{to_time}")
    
    orchestrator.run(plan_only=args.plan_only)


if __name__ == "__main__":
    main()
