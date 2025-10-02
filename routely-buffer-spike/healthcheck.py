#!/usr/bin/env python3
"""
Healthcheck script to verify RDM API key works with a small, predictable request.
Uses BTN→VIC 07:00-08:00 as a minimal test case.
"""

import os
import sys
from rdm_client import RDMClient

def main():
    print("🔍 RDM API Healthcheck")
    print("=" * 40)
    
    try:
        # Initialize client
        client = RDMClient()
        print("✅ RDM client initialized")
        
        # Small, predictable request: Brighton to Victoria, single hour
        print("📡 Testing with BTN→VIC 0700-0800 on 2025-02-14...")
        
        result = client.get_metrics(
            from_loc="BTN",
            to_loc="VIC", 
            from_time="0700",
            to_time="0800",
            from_date="2025-02-14",
            to_date="2025-02-14",
            days="WEEKDAY"
        )
        
        # Success case
        services = result.get("Services", [])
        print(f"✅ SUCCESS: HTTP 200")
        print(f"📊 Services found: {len(services)}")
        
        if services:
            # Show first 3 RIDs
            rids = []
            for service in services[:3]:
                rid = service.get("rid")
                if rid:
                    rids.append(rid)
            
            print(f"🆔 First 3 RIDs: {rids}")
        else:
            print("ℹ️  No services found (empty result set)")
            
        print("\n🎯 DIAGNOSIS: Auth key works, 403s are likely policy/rate limiting")
        
    except RuntimeError as e:
        error_msg = str(e)
        print(f"❌ ERROR: {error_msg}")
        
        # Parse HTTP status from error message
        if "HTTP 403" in error_msg:
            print("\n🚨 DIAGNOSIS: 403 on small request - key blocked/expired/propagation issue")
            print("   → Pause, rotate key, or contact RDM")
        elif "HTTP 429" in error_msg:
            print("\n⏱️  DIAGNOSIS: Rate limited - need throttling")
        elif "HTTP 5" in error_msg:
            print("\n🔧 DIAGNOSIS: Server error - try again later")
        else:
            print(f"\n🔍 DIAGNOSIS: Unexpected error - {error_msg[:300]}")
            
        # Show response body preview
        if ":" in error_msg:
            response_preview = error_msg.split(":", 1)[1][:300]
            print(f"📄 Response preview: {response_preview}")
            
        sys.exit(1)
        
    except Exception as e:
        print(f"💥 UNEXPECTED ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
