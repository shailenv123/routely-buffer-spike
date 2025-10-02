#!/usr/bin/env python3
"""
Quick RDM API diagnostic - focused on the most likely issues.
"""

import os
import requests
import json

def main():
    print("🔍 QUICK RDM API DIAGNOSTIC")
    print("=" * 40)
    
    # Check for API key
    api_key = os.getenv("RDM_API_KEY")
    if not api_key:
        print("❌ RDM_API_KEY not found in environment variables")
        print("💡 Set it with: export RDM_API_KEY='your_key_here'")
        return
    
    # Mask key for display
    masked = f"{api_key[:6]}...{api_key[-6:]}" if len(api_key) >= 12 else "[SHORT_KEY]"
    print(f"✅ API Key found: {masked} (length: {len(api_key)})")
    
    # Quick test with minimal payload
    url = "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1/serviceMetrics"
    headers = {
        "Content-Type": "application/json",
        "x-apikey": api_key
    }
    body = {
        "from_loc": "BTN",
        "to_loc": "VIC", 
        "from_time": "0700",
        "to_time": "0710",  # Small window
        "from_date": "2025-02-14",
        "to_date": "2025-02-14",
        "days": "WEEKDAY"
    }
    
    print(f"\n📡 Testing: {url}")
    print(f"🔑 Header: x-apikey: {masked}")
    print(f"📦 Body: {json.dumps(body, separators=(',', ':'))}")
    
    try:
        response = requests.post(url, headers=headers, json=body, timeout=15)
        print(f"\n📊 RESULT:")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            print("✅ SUCCESS: API key and endpoint working")
        elif response.status_code == 403:
            print("🚨 403 FORBIDDEN: Key blocked or gateway protection")
            print("💡 Possible causes:")
            print("   - API key needs to be rebound to HSP product")
            print("   - Gateway rate limiting/protection")
            print("   - Key expired or suspended")
        else:
            print(f"⚠️  Unexpected status: {response.status_code}")
            
    except requests.exceptions.Timeout:
        print("⏰ TIMEOUT: Request took >15s")
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    main()
