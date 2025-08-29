#!/usr/bin/env python3
"""
Self-check probe for RDM API connectivity.

Tests basic API functionality with a minimal request to verify:
- API key authentication
- Network connectivity  
- Service availability
"""

import os
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv


def main():
    """Run self-check probe."""
    print("🔍 RDM API Self-Check Probe")
    print("=" * 40)
    
    # Load API key
    load_dotenv()
    api_key = os.getenv("RDM_API_KEY")
    if not api_key:
        print("❌ RDM_API_KEY not found in .env file")
        return False
    
    print(f"✅ API key loaded: {api_key[:4]}...{api_key[-4:]}")
    
    # Build headers
    headers = {
        "Content-Type": "application/json",
        "x-apikey": api_key,
        "User-Agent": "Routely-RDM/1.0"
    }
    
    # Calculate a recent weekday (7 days ago)
    target_date = datetime.now().date() - timedelta(days=7)
    
    # Ensure it's a weekday (Monday-Friday)
    while target_date.weekday() > 4:  # 0-4 = Mon-Fri, 5-6 = Sat-Sun
        target_date -= timedelta(days=1)
    
    date_str = target_date.isoformat()
    print(f"📅 Testing date: {date_str} ({target_date.strftime('%A')})")
    
    # Build minimal test payload (10-minute window)
    payload = {
        "from_loc": "PAD",
        "to_loc": "BRI", 
        "from_time": "0900",
        "to_time": "0910",
        "from_date": date_str,
        "to_date": date_str,
        "days": "WEEKDAY"
    }
    
    print(f"🎯 Test route: PAD→BRI")
    print(f"⏰ Time window: 09:00-09:10 (10 minutes)")
    print()
    
    # Make API call
    url = "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1/serviceMetrics"
    
    print("🚀 Making API call...")
    start_time = time.time()
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        elapsed_ms = int((time.time() - start_time) * 1000)
        
        # Parse response
        if response.status_code == 200:
            data = response.json()
            services = data.get("Services", [])
            services_count = len(services)
            
            print(f"✅ HTTP Status: {response.status_code}")
            print(f"⚡ Elapsed: {elapsed_ms}ms")
            print(f"📊 Services found: {services_count}")
            
            if services_count > 0:
                print("🎉 Self-check PASSED - API is working!")
                
                # Show sample service info
                sample_service = services[0]
                attrs = sample_service.get("serviceAttributesMetrics", {})
                print(f"📋 Sample: {attrs.get('origin_location', 'N/A')}→{attrs.get('destination_location', 'N/A')} "
                      f"at {attrs.get('gbtt_ptd', 'N/A')} ({len(attrs.get('rids', []))} RIDs)")
                
                return True
            else:
                print("⚠️  Self-check WARNING - No services found for this time window")
                print("💡 Try a different date or wider time window")
                return False
                
        else:
            print(f"❌ HTTP Status: {response.status_code}")
            print(f"⚡ Elapsed: {elapsed_ms}ms") 
            print(f"📄 Response: {response.text[:200]}...")
            print("❌ Self-check FAILED - API error")
            return False
            
    except requests.exceptions.Timeout:
        elapsed_ms = int((time.time() - start_time) * 1000)
        print(f"❌ Request timeout after {elapsed_ms}ms")
        print("❌ Self-check FAILED - Network timeout")
        return False
        
    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        print(f"❌ Error after {elapsed_ms}ms: {e}")
        print("❌ Self-check FAILED - Unexpected error")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
