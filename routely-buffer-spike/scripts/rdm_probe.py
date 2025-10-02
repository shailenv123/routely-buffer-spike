#!/usr/bin/env python3
"""
Canonical RDM API probe script.
Tests the exact request shape that previously worked to isolate 403 issues.
"""

import os
import json
import requests
from dotenv import load_dotenv


def probe_rdm_api():
    """Send a minimal probe request to test API connectivity."""
    
    # Load environment
    load_dotenv()
    api_key = os.getenv("RDM_API_KEY")
    
    if not api_key:
        print("❌ RDM_API_KEY not found in environment")
        return False
    
    # Mask key for logging
    if len(api_key) >= 12:
        masked_key = api_key[:6] + "…" + api_key[-6:]
    else:
        masked_key = "[SHORT_KEY]"
    
    print(f"🔑 Using API key: {masked_key}")
    
    # Construct the canonical request
    base_url = "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1"
    url = f"{base_url}/serviceMetrics"
    
    headers = {
        "Content-Type": "application/json",
        "x-apikey": api_key,
        "User-Agent": "Routely-RDM/1.0"
    }
    
    # Use a known working payload format
    payload = {
        "from_loc": "BTN",
        "to_loc": "VIC", 
        "from_time": "0700",
        "to_time": "0800",
        "from_date": "2025-02-14",
        "to_date": "2025-02-14",
        "days": "WEEKDAY"
    }
    
    print(f"🎯 Target URL: {url}")
    print(f"📋 Payload: {json.dumps(payload, indent=2)}")
    print(f"📤 Headers: Content-Type={headers['Content-Type']}, x-apikey={masked_key}, User-Agent={headers['User-Agent']}")
    
    try:
        print("\n🚀 Sending request...")
        response = requests.post(url, json=payload, headers=headers, timeout=25)
        
        print(f"📊 Status Code: {response.status_code}")
        print(f"📝 Response Headers: {dict(response.headers)}")
        
        # Show first 300 chars of response body
        response_text = response.text
        if len(response_text) > 300:
            truncated_body = response_text[:300] + "..."
        else:
            truncated_body = response_text
            
        print(f"📄 Response Body (first 300 chars): {truncated_body}")
        
        if response.status_code == 200:
            print("✅ SUCCESS: API accepts the canonical request")
            try:
                data = response.json()
                print(f"📊 Response contains {len(data)} items" if isinstance(data, list) else "📊 Response is a dict")
            except json.JSONDecodeError:
                print("⚠️  Response is not valid JSON")
            return True
        else:
            print(f"❌ FAILURE: API returned {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"💥 Request failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = probe_rdm_api()
    if success:
        print("\n🎉 Canonical probe successful - API key and request format are working")
    else:
        print("\n💥 Canonical probe failed - this indicates the core API issue")
