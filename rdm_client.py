import os
import time
import requests
from typing import Dict, Any
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
        
        # Create session with default headers
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "x-apikey": api_key,
            "User-Agent": "Routely-RDM/1.0"
        })
        
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
        self.metrics_timeout = 90  # seconds
        self.details_timeout = 30  # seconds
    
    def set_metrics_timeout(self, seconds: int):
        """Set timeout for metrics API calls."""
        self.metrics_timeout = seconds
    
    def set_details_timeout(self, seconds: int):
        """Set timeout for details API calls."""
        self.details_timeout = seconds
    
    def get_metrics(self, from_loc: str, to_loc: str, from_time: str, to_time: str, 
                   from_date: str, to_date: str, days: str) -> Dict[str, Any]:
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
        
        # Manual retry loop with exponential backoff
        retry_delays = [1, 2, 4]  # seconds
        
        for attempt in range(len(retry_delays) + 1):  # 3 retries + initial attempt
            try:
                resp = self.session.post(url, json=payload, timeout=self.metrics_timeout)
                
                if resp.status_code == 200:
                    return resp.json()
                else:
                    # Truncate response body to first 400 characters
                    error_body = resp.text[:400]
                    error_msg = f"HTTP {resp.status_code}: {error_body}"
                    
                    # If this is the last attempt, raise the error
                    if attempt == len(retry_delays):
                        raise RuntimeError(error_msg)
                    
                    # Otherwise, wait and retry
                    time.sleep(retry_delays[attempt])
                    continue
                    
            except requests.exceptions.RequestException as e:
                # If this is the last attempt, raise the error
                if attempt == len(retry_delays):
                    raise RuntimeError(f"Request failed: {str(e)}")
                
                # Otherwise, wait and retry
                time.sleep(retry_delays[attempt])
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
                    # Truncate response body to first 400 characters
                    error_body = resp.text[:400]
                    error_msg = f"HTTP {resp.status_code}: {error_body}"
                    
                    # If this is the last attempt, raise the error
                    if attempt == len(retry_delays):
                        raise RuntimeError(error_msg)
                    
                    # Otherwise, wait and retry
                    time.sleep(retry_delays[attempt])
                    continue
                    
            except requests.exceptions.RequestException as e:
                # If this is the last attempt, raise the error
                if attempt == len(retry_delays):
                    raise RuntimeError(f"Request failed: {str(e)}")
                
                # Otherwise, wait and retry
                time.sleep(retry_delays[attempt])
                continue
        
        # This should never be reached, but just in case
        raise RuntimeError("Maximum retries exceeded")
