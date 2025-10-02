#!/usr/bin/env python3
"""
RDM API Diagnostic Tool
Comprehensive testing to identify API key, gateway, or endpoint issues.
"""

import os
import sys
import time
import json
import base64
import requests
from pathlib import Path
from dotenv import load_dotenv

def mask_key(key):
    """Mask API key for safe printing."""
    if not key or len(key) < 12:
        return "[INVALID_KEY]"
    return f"{key[:6]}...{key[-6:]}"

def check_env_file():
    """Check .env file for encoding issues and key format."""
    print("🔍 ENVIRONMENT & KEY SANITY CHECK")
    print("=" * 50)
    
    env_path = Path(".env")
    env_exists = env_path.exists()
    
    if env_exists:
        # Read raw bytes to check for BOM
        raw_bytes = env_path.read_bytes()
        has_bom = raw_bytes.startswith(b'\xef\xbb\xbf')
        
        print(f"📄 .env encoding hints:")
        print(f"   • .env file: FOUND")
        print(f"   • BOM present: {'YES' if has_bom else 'NO'}")
        print(f"   • File size: {len(raw_bytes)} bytes")
        
        # Load environment from file
        load_dotenv()
    else:
        print(f"📄 .env file: NOT FOUND (checking system environment)")
    
    # Check for API key in environment (from .env or system)
    raw_key = os.getenv("RDM_API_KEY")
    
    if not raw_key:
        print("❌ RDM_API_KEY not found in environment")
        return None
    
    # Check for quotes and whitespace
    trimmed_key = raw_key.strip().strip('"').strip("'")
    has_quotes = raw_key != trimmed_key
    has_whitespace = raw_key != raw_key.strip()
    
    key_source = ".env file" if env_exists else "system environment"
    print(f"🔑 API Key analysis:")
    print(f"   • Key present: YES (from {key_source})")
    print(f"   • Raw length: {len(raw_key)}")
    print(f"   • Trimmed length: {len(trimmed_key)}")
    print(f"   • Masked fingerprint: {mask_key(trimmed_key)}")
    print(f"   • Has quotes: {'YES' if has_quotes else 'NO'}")
    print(f"   • Has whitespace: {'YES' if has_whitespace else 'NO'}")
    
    if has_quotes or has_whitespace:
        print("⚠️  Key formatting issue detected - using trimmed version")
    
    return trimmed_key

def run_test(test_id, description, url, headers, body, timeout=25):
    """Run a single API test."""
    print(f"\n🧪 {test_id}: {description}")
    print(f"   URL: {url}")
    
    # Mask headers for printing
    safe_headers = headers.copy()
    for key in safe_headers:
        if 'apikey' in key.lower():
            safe_headers[key] = mask_key(safe_headers[key])
    print(f"   Headers: {safe_headers}")
    print(f"   Body: {json.dumps(body, separators=(',', ':'))}")
    
    try:
        response = requests.post(url, headers=headers, json=body, timeout=timeout)
        
        # Extract Apigee diagnostic headers
        apigee_headers = {k: v for k, v in response.headers.items() 
                         if k.lower().startswith('x-apigee')}
        
        result = {
            'test_id': test_id,
            'status': response.status_code,
            'response_text': response.text[:300],
            'apigee_headers': apigee_headers,
            'success': 200 <= response.status_code < 300
        }
        
        print(f"   Status: {response.status_code}")
        print(f"   Response: {result['response_text']}")
        if apigee_headers:
            print(f"   Apigee headers: {apigee_headers}")
        
        return result
        
    except requests.exceptions.Timeout:
        print(f"   TIMEOUT {timeout}s")
        return {'test_id': test_id, 'status': 'TIMEOUT', 'success': False}
    except Exception as e:
        print(f"   ERROR: {e}")
        return {'test_id': test_id, 'status': 'ERROR', 'success': False, 'error': str(e)}

def test_nrdp_control(api_key):
    """Test NRDP as a control to verify client/network/payload."""
    nrdp_user = os.getenv("NRDP_USER")
    nrdp_pass = os.getenv("NRDP_PASS")
    
    if not nrdp_user or not nrdp_pass:
        print("\n🔬 NRDP CONTROL TEST: Skipped (no NRDP credentials)")
        return None
    
    print("\n🔬 NRDP CONTROL TEST")
    print("-" * 30)
    
    url = "https://hsp-prod.rockshore.net/api/v1/serviceMetrics"
    auth_string = base64.b64encode(f"{nrdp_user}:{nrdp_pass}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_string}"
    }
    body = {
        "from_loc": "BTN",
        "to_loc": "VIC", 
        "from_time": "0700",
        "to_time": "0800",
        "from_date": "2025-02-14",
        "to_date": "2025-02-14",
        "days": "WEEKDAY"
    }
    
    return run_test("NRDP", "Control test via NRDP", url, headers, body)

def analyze_results(results):
    """Analyze test results and provide diagnosis."""
    print("\n📊 RESULTS SUMMARY")
    print("=" * 50)
    
    # Print compact table
    print(f"{'Test':<6} {'Status':<8} {'Note':<40}")
    print("-" * 54)
    
    success_tests = []
    for result in results:
        if result is None:
            continue
        status = str(result['status'])
        note = "OK" if result.get('success') else "Failed"
        if 'error' in result:
            note = f"Error: {result['error'][:30]}"
        print(f"{result['test_id']:<6} {status:<8} {note:<40}")
        
        if result.get('success'):
            success_tests.append(result['test_id'])
    
    print("\n🎯 DIAGNOSIS")
    print("=" * 50)
    
    if not results or all(r is None or not r.get('success') for r in results if r):
        if any(r and r.get('test_id') == 'NRDP' and r.get('success') for r in results if r):
            print("🚨 Key bound/blocked at RDM gateway; contact RDM to rebind key to product")
            return "Contact RDM support to rebind API key to HSP product"
        else:
            print("🚨 All tests failed - likely API key or network issue")
            return "Verify API key validity and network connectivity"
    
    # Check specific success patterns
    if 'T1' in success_tests or 'T2' in success_tests:
        print("✅ RDM OK (minimal canonical request accepted)")
        return "API is working - investigate application-specific issues"
    
    if 'T7' in success_tests and 'T1' not in success_tests:
        print("🔧 Gateway expects x-api-key instead of x-apikey for this product")
        return "Switch header name from x-apikey to x-api-key"
    
    if 'T3' in success_tests and 'T1' not in success_tests:
        print("🔧 Endpoint path expects trailing slash")
        return "Use /serviceMetrics/ instead of /serviceMetrics"
    
    if 'T4' in success_tests and 'T1' not in success_tests:
        print("📏 Payload cardinality too large")
        return "Keep windows ≤10 minutes or enable adaptive splitting"
    
    if 'T5' in success_tests and 'T1' not in success_tests:
        print("🛣️  Route-specific issue detected")
        return "BTN→VIC route may have issues; try alternative routes"
    
    print("🤔 Partial success - review individual test results")
    return "Review test results for specific failure patterns"

def main():
    """Main diagnostic routine."""
    start_time = time.time()
    
    print("🔍 RDM API DIAGNOSTIC TOOL")
    print("=" * 60)
    
    # Step 1: Environment check
    api_key = check_env_file()
    if not api_key:
        sys.exit(1)
    
    # Step 2: Test matrix
    print(f"\n🧪 API TEST MATRIX")
    print("=" * 50)
    
    base_url = "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1"
    canonical_body = {
        "from_loc": "BTN",
        "to_loc": "VIC",
        "from_time": "0700", 
        "to_time": "0800",
        "from_date": "2025-02-14",
        "to_date": "2025-02-14",
        "days": "WEEKDAY"
    }
    
    results = []
    
    # T1: Canonical
    results.append(run_test(
        "T1", "Canonical request",
        f"{base_url}/serviceMetrics",
        {"Content-Type": "application/json", "x-apikey": api_key},
        canonical_body
    ))
    
    # Early exit if T1 succeeds
    if results[-1] and results[-1].get('success'):
        print("\n✅ T1 succeeded - API is working normally")
    else:
        # T2: Add polite headers
        results.append(run_test(
            "T2", "With polite headers",
            f"{base_url}/serviceMetrics",
            {
                "Content-Type": "application/json", 
                "Accept": "application/json",
                "User-Agent": "RoutelyBuffer/0.1 (ops@yourdomain)",
                "x-apikey": api_key
            },
            canonical_body
        ))
        
        # T3: Trailing slash
        results.append(run_test(
            "T3", "Trailing slash path", 
            f"{base_url}/serviceMetrics/",
            {"Content-Type": "application/json", "x-apikey": api_key},
            canonical_body
        ))
        
        # T4: Tiny window
        tiny_body = canonical_body.copy()
        tiny_body["to_time"] = "0710"
        results.append(run_test(
            "T4", "Tiny window (10min)",
            f"{base_url}/serviceMetrics",
            {"Content-Type": "application/json", "x-apikey": api_key},
            tiny_body
        ))
        
        # T5: Alternative route
        alt_body = canonical_body.copy()
        alt_body.update({"from_loc": "PAD", "to_loc": "BRI", "to_time": "0710"})
        results.append(run_test(
            "T5", "Alternative route PAD→BRI",
            f"{base_url}/serviceMetrics",
            {"Content-Type": "application/json", "x-apikey": api_key},
            alt_body
        ))
        
        # T6: Only x-apikey header
        results.append(run_test(
            "T6", "Only x-apikey header",
            f"{base_url}/serviceMetrics", 
            {"x-apikey": api_key},
            canonical_body
        ))
        
        # T7: Alternative spelling x-api-key
        results.append(run_test(
            "T7", "Alternative spelling x-api-key",
            f"{base_url}/serviceMetrics",
            {"x-api-key": api_key},
            canonical_body
        ))
        
        # T8: Both spellings (forensics)
        results.append(run_test(
            "T8", "Both spellings (forensics)",
            f"{base_url}/serviceMetrics",
            {
                "Content-Type": "application/json",
                "x-apikey": api_key,
                "x-api-key": api_key
            },
            canonical_body
        ))
    
    # Step 3: NRDP control test
    nrdp_result = test_nrdp_control(api_key)
    if nrdp_result:
        results.append(nrdp_result)
    
    # Step 4: Analysis
    recommended_action = analyze_results(results)
    
    # Final summary
    elapsed = time.time() - start_time
    script_path = Path(__file__).absolute()
    
    print(f"\n📋 RECOMMENDED NEXT ACTION")
    print("=" * 50)
    print(f"➡️  {recommended_action}")
    
    print(f"\n📍 Script location: {script_path}")
    print(f"⏱️  Time taken: {elapsed:.2f}s")

if __name__ == "__main__":
    main()
