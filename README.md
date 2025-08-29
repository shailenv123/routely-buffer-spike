# Routely Buffer Spike

A data analysis project for processing rail delay data using the RDM API to provide AI-powered buffer time recommendations for rail routes.

## Requirements

**Python Version:** Python 3.13.7

**Install Options:**
- **Dev:** `pip install -r requirements.txt` (minimum versions)
- **Repro:** `pip install -r requirements.lock.txt` (exact versions)

## Setup

### Windows PowerShell Setup

1. Create and activate virtual environment:
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

2. Install dependencies:
```powershell
pip install -r requirements.txt
```

3. Create environment file:
```powershell
Copy-Item .env.example .env
# Edit .env and paste your key: RDM_API_KEY=your_actual_key_here
```

### Required Headers
The RDM API requires these headers:
- `x-apikey`: Your RDM API key
- `Content-Type: application/json`
- `User-Agent`: Any valid user agent string

## Usage

### Data Collection

#### 1-Day Validation Run (Yesterday)
```powershell
python pipeline.py --days 1
```

**Expected Output:**
- One file: `data/raw_delays/delays_YYYY-MM-DD.csv.gz` 
- Percentiles: `data/route_hour_p80_p90_p95.csv`
- Summary: Total RIDs fetched, delay rows, percentile groups

#### 30-Day Batch Run (Final for this spike)
```powershell
python pipeline.py --days 30
```

**Expected Output:**
- 30 daily files in `data/raw_delays/`
- Comprehensive percentile analysis across all routes and time periods

### Interactive UI

Launch the Streamlit app for route buffer recommendations:
```powershell
streamlit run app.py
```

The app provides:
- Route selection (VIC→GTW, PAD→HXX, etc.)
- Hour and day-of-week selection
- Risk tolerance slider (80-99%)
- Real-time buffer calculations with percentile interpolation
- Data export functionality

## Project Structure

```
routely-buffer-spike/
├── data/
│   ├── raw_delays/           # Daily CSV files (delays_YYYY-MM-DD.csv.gz)
│   └── route_hour_p80_p90_p95.csv  # Percentile analysis output
├── notebooks/
│   └── 01_build_percentiles.ipynb  # Analysis notebook
├── pipeline.py               # Main data collection orchestrator
├── app.py                   # Streamlit web interface
├── rdm_client.py            # RDM API client with retry logic
├── routes.py                # Route and time bucket configuration
├── ingest_day.py            # Daily data ingestion
├── build_percentiles.py     # Percentile computation
├── requirements.txt         # Python dependencies
├── .env.example            # API key template
└── README.md               # This file
```

## Key Features

- **Automated data collection** from RDM Historical Service Performance API
- **Concurrent processing** with rate limiting and error handling
- **Percentile-based analysis** by route, hour, and day of week
- **Interactive web interface** for buffer recommendations
- **Piecewise linear interpolation** for precise risk-based calculations
- **Data export** capabilities for further analysis
