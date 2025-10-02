#!/usr/bin/env python3
"""
State and log hygiene utility for rail delay data collection.
Provides options for cleaning execution state to enable retries or full re-runs.
"""

import os
import sys
import json
import shutil
import argparse
from pathlib import Path
from typing import Dict, List


def clean_failed_slices(state_file: str = "state/state.json") -> int:
    """
    Remove only failed slice markers to allow retry.
    Keeps successful slices to avoid unnecessary re-processing.
    
    Returns:
        Number of failed slices cleared
    """
    if not os.path.exists(state_file):
        print("📄 No state file found - nothing to clean")
        return 0
    
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
    except Exception as e:
        print(f"❌ Error reading state file: {e}")
        return 0
    
    slices = state.get("slices", {})
    failed_count = 0
    cleared_slices = []
    
    # Find and clear failed slices
    for slice_key, status in list(slices.items()):
        if status in ['failed', 'timeout', 'circuit_breaker']:
            del slices[slice_key]
            cleared_slices.append(slice_key)
            failed_count += 1
            
            # Also remove associated metadata
            if "slice_metadata" in state and slice_key in state["slice_metadata"]:
                del state["slice_metadata"][slice_key]
    
    if failed_count > 0:
        # Save updated state
        try:
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            print(f"✅ Cleared {failed_count} failed slice(s) from state")
            
            # Show first few cleared slices for confirmation
            if len(cleared_slices) <= 5:
                for slice_key in cleared_slices:
                    print(f"   • {slice_key}")
            else:
                for slice_key in cleared_slices[:3]:
                    print(f"   • {slice_key}")
                print(f"   • ... and {len(cleared_slices) - 3} more")
                
        except Exception as e:
            print(f"❌ Error saving updated state: {e}")
            return 0
    else:
        print("📋 No failed slices found in state")
    
    return failed_count


def clean_hard(state_dir: str = "state", logs_dir: str = "logs") -> Dict[str, int]:
    """
    Remove state/ and logs/ directories completely to force full re-run.
    
    Returns:
        Dictionary with counts of removed items
    """
    results = {
        "state_files": 0,
        "log_files": 0,
        "directories": 0
    }
    
    # Remove state directory
    if os.path.exists(state_dir):
        try:
            # Count files before removal
            state_path = Path(state_dir)
            results["state_files"] = len(list(state_path.rglob("*")))
            
            shutil.rmtree(state_dir)
            results["directories"] += 1
            print(f"🗑️  Removed state directory: {state_dir} ({results['state_files']} files)")
        except Exception as e:
            print(f"❌ Error removing state directory: {e}")
    else:
        print(f"📂 State directory not found: {state_dir}")
    
    # Remove logs directory  
    if os.path.exists(logs_dir):
        try:
            # Count files before removal
            logs_path = Path(logs_dir)
            results["log_files"] = len(list(logs_path.rglob("*")))
            
            shutil.rmtree(logs_dir)
            results["directories"] += 1
            print(f"🗑️  Removed logs directory: {logs_dir} ({results['log_files']} files)")
        except Exception as e:
            print(f"❌ Error removing logs directory: {e}")
    else:
        print(f"📂 Logs directory not found: {logs_dir}")
    
    return results


def show_state_summary(state_file: str = "state/state.json"):
    """Show summary of current execution state."""
    if not os.path.exists(state_file):
        print("📄 No state file found")
        return
    
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
    except Exception as e:
        print(f"❌ Error reading state file: {e}")
        return
    
    slices = state.get("slices", {})
    metadata = state.get("metadata", {})
    
    if not slices:
        print("📋 State file exists but contains no slice information")
        return
    
    # Count by status
    status_counts = {}
    for status in slices.values():
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print("📊 Current State Summary:")
    print(f"   Created: {metadata.get('created', 'unknown')}")
    print(f"   Total slices: {len(slices)}")
    
    for status, count in sorted(status_counts.items()):
        emoji = {
            'ok': '✅',
            'pending': '⏳',
            'failed': '❌',
            'skipped': '⏭️',
            'timeout': '⏰',
            'circuit_breaker': '🔴',
            'bisected': '🔄'
        }.get(status, '❓')
        print(f"   {emoji} {status}: {count}")


def clean_reports(reports_dir: str = "reports", keep_latest: int = 3) -> int:
    """
    Clean old QA reports, keeping only the most recent ones.
    
    Args:
        reports_dir: Directory containing QA reports
        keep_latest: Number of latest reports to keep
        
    Returns:
        Number of reports removed
    """
    if not os.path.exists(reports_dir):
        print(f"📂 Reports directory not found: {reports_dir}")
        return 0
    
    reports_path = Path(reports_dir)
    qa_reports = list(reports_path.glob("qa_*.html"))
    
    if len(qa_reports) <= keep_latest:
        print(f"📄 Found {len(qa_reports)} reports (≤ {keep_latest} to keep)")
        return 0
    
    # Sort by modification time, newest first
    qa_reports.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    # Remove older reports
    removed_count = 0
    for report in qa_reports[keep_latest:]:
        try:
            report.unlink()
            removed_count += 1
            print(f"🗑️  Removed old report: {report.name}")
        except Exception as e:
            print(f"❌ Error removing {report.name}: {e}")
    
    print(f"✅ Cleaned {removed_count} old reports, kept {keep_latest} latest")
    return removed_count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="State and log hygiene utility for rail delay data collection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/clean_state.py                    # Clear failed slices for retry
  python scripts/clean_state.py --hard             # Full clean (removes all state/logs)
  python scripts/clean_state.py --status           # Show current state summary
  python scripts/clean_state.py --clean-reports    # Clean old QA reports
        """
    )
    
    parser.add_argument(
        "--hard",
        action="store_true",
        help="Remove state/ and logs/ directories completely (forces full re-run)"
    )
    
    parser.add_argument(
        "--status",
        action="store_true", 
        help="Show current state summary without making changes"
    )
    
    parser.add_argument(
        "--clean-reports",
        action="store_true",
        help="Clean old QA reports (keeps 3 most recent)"
    )
    
    parser.add_argument(
        "--keep-reports",
        type=int,
        default=3,
        help="Number of recent QA reports to keep when cleaning (default: 3)"
    )
    
    parser.add_argument(
        "--state-file",
        default="state/state.json",
        help="Path to state file (default: state/state.json)"
    )
    
    args = parser.parse_args()
    
    print("🧹 Rail Delay Data - State Hygiene Utility")
    print("==========================================")
    print()
    
    if args.status:
        show_state_summary(args.state_file)
        return
    
    if args.clean_reports:
        clean_reports(keep_latest=args.keep_reports)
        return
    
    if args.hard:
        print("⚠️  HARD CLEAN: This will remove ALL execution state and logs")
        print("   The next run will start completely fresh.")
        print()
        
        # Confirmation prompt
        try:
            response = input("Continue? [y/N]: ").strip().lower()
            if response != 'y':
                print("❌ Aborted")
                return
        except KeyboardInterrupt:
            print("\n❌ Aborted")
            return
        
        results = clean_hard()
        total_files = results["state_files"] + results["log_files"]
        
        print()
        print("✅ Hard clean complete:")
        print(f"   🗑️  Removed {results['directories']} directories")
        print(f"   📄 Removed {total_files} files total")
        print("   🔄 Next run will start fresh")
        
    else:
        # Default: clean only failed slices
        print("🔄 SOFT CLEAN: Clearing failed slices to allow retry")
        print("   Successful slices will be preserved.")
        print()
        
        failed_count = clean_failed_slices(args.state_file)
        
        if failed_count > 0:
            print()
            print("✅ Soft clean complete - failed slices cleared for retry")
        else:
            print("✅ No failed slices to clean")
        
        print()
        print("💡 Tip: Use --hard to force a complete re-run")
        print("       Use --status to see current execution state")


if __name__ == "__main__":
    main()
