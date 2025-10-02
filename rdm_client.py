import os
import time
import random
import requests
from typing import Dict, Any, List
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RDMClient:
    """RDM API client with connection pooling, retries, and parameterized timeouts."""
    
    BASE = "https://api1.raildata.org.uk/1010-historical-service-performance-_hsp_v1/api/v1"
    
    def __init__(self):
        """Initialize RDM client with API key, session, and connection pooling."""
        load_dotenv()
        api_key = os.getenv("RDM_API_KEY")
        if not api_key:
            raise RuntimeError("RDM_API_KEY not found in environment variables")
        
        # Verbose logging (controlled by environment)
        self.verbose = os.getenv("RDM_VERBOSE", "0") == "1"
        
        # Create session with default headers
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "x-apikey": api_key,
            "User-Agent": "Routely-RDM/1.0"
        })
        
        # Optionally log configuration for diagnostics
        if self.verbose:
            print(f"[RDM] BASE={self.BASE}")
            key = self.session.headers.get("x-apikey", "")
            masked = key[:6] + "…" + key[-6:] if len(key) >= 12 else "[SHORT_KEY]"
            print(f"[RDM] Headers: Content-Type={self.session.headers.get('Content-Type')}, x-apikey={masked}, User-Agent={self.session.headers.get('User-Agent')}")
        
        # Configure connection pooling and retry policy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        
        adapter = HTTPAdapter(
            pool_connections=64,
            pool_maxsize=64,
            max_retries=retry_strategy
        )
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Default timeouts
        self.metrics_timeout = 120  # seconds
        self.details_timeout = 30   # seconds
    
    def set_metrics_timeout(self, seconds: int):
        """Set timeout for metrics API calls."""
        self.metrics_timeout = seconds
    
    def set_details_timeout(self, seconds: int):
        """Set timeout for details API calls."""
        self.details_timeout = seconds
    
    def get_metrics(self, from_loc: str, to_loc: str, from_time: str, to_time: str, 
                   from_date: str, to_date: str, days: str, toc_filter: List[str] = None, 
                   timeout: int = None) -> Dict[str, Any]:
        """Get service metrics for a route and time period."""
        url = f"{self.BASE}/serviceMetrics"
        payload = {
            "from_loc": from_loc,
            "to_loc": to_loc,
            "from_time": from_time,
            "to_time": to_time,
            "from_date": from_date,
            "to_date": to_date,
            "days": days
        }
        
        # Add TOC filter if provided
        if toc_filter is not None:
            payload["toc_filter"] = toc_filter
        
        # Manual retry loop with exponential backoff
        retry_delays = [1, 2, 4]  # seconds
        
        # Use provided timeout or default
        request_timeout = timeout if timeout is not None else self.metrics_timeout
        
        for attempt in range(len(retry_delays) + 1):  # 3 retries + initial attempt
            try:
                if self.verbose and attempt == 0:  # Log once per process
                    import json as json_mod
                    print(f"[RDM] Sending request to {url}")
                    print(f"[RDM] Payload: {json_mod.dumps(payload, indent=2)}")
                
                resp = self.session.post(url, json=payload, timeout=request_timeout)
                
                if self.verbose and attempt == 0:  # Log once per process
                    print(f"[RDM] Response status: {resp.status_code}")
                    if resp.status_code != 200:
                        error_preview = resp.text[:200] if resp.text else "[No content]"
                        print(f"[RDM] Error preview: {error_preview}")
                
                if resp.status_code == 200:
                    return resp.json()
                else:
                    # Truncate response body to first 300 characters
                    error_body = resp.text[:300]
                    error_msg = f"HTTP {resp.status_code} from {url}: {error_body}"
                    
                    # If this is the last attempt, raise the error
                    if attempt == len(retry_delays):
                        raise RuntimeError(error_msg)
                    
                    # Otherwise, wait and retry with jitter
                    jitter = random.uniform(0, 0.4)
                    time.sleep(retry_delays[attempt] + jitter)
                    continue
                    
            except requests.exceptions.RequestException as e:
                # If this is the last attempt, raise the error
                if attempt == len(retry_delays):
                    raise RuntimeError(f"Request failed: {str(e)}")
                
                # Otherwise, wait and retry with jitter
                jitter = random.uniform(0, 0.4)
                time.sleep(retry_delays[attempt] + jitter)
                continue
        
        # This should never be reached, but just in case
        raise RuntimeError("Maximum retries exceeded")
    
    def get_details(self, rid: str) -> Dict[str, Any]:
        """Get service details for a specific RID."""
        url = f"{self.BASE}/serviceDetails"
        body = {"rid": str(rid)}
        
        # Manual retry loop with exponential backoff
        retry_delays = [1, 2, 4]  # seconds
        
        for attempt in range(len(retry_delays) + 1):  # 3 retries + initial attempt
            try:
                resp = self.session.post(url, json=body, timeout=self.details_timeout)
                
                if resp.status_code == 200:
                    return resp.json()
                else:
                    # Truncate response body to first 300 characters
                    error_body = resp.text[:300]
                    error_msg = f"HTTP {resp.status_code} from {url}: {error_body}"
                    
                    # If this is the last attempt, raise the error
                    if attempt == len(retry_delays):
                        raise RuntimeError(error_msg)
                    
                    # Otherwise, wait and retry with jitter
                    jitter = random.uniform(0, 0.4)
                    time.sleep(retry_delays[attempt] + jitter)
                    continue
                    
            except requests.exceptions.RequestException as e:
                # If this is the last attempt, raise the error
                if attempt == len(retry_delays):
                    raise RuntimeError(f"Request failed: {str(e)}")
                
                # Otherwise, wait and retry with jitter
                jitter = random.uniform(0, 0.4)
                time.sleep(retry_delays[attempt] + jitter)
                continue
        
        # This should never be reached, but just in case
        raise RuntimeError("Maximum retries exceeded")
