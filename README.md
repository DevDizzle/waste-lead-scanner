# Waste Lead Scanner — MVP
## Permit-based lead generation for waste management companies

Scans building permits via Shovels.ai API, filters to waste-generating permit types, enriches with contractor details, scores leads by estimated waste volume, and serves via Streamlit dashboard.

**Pilot client:** Arrow Waste Services (Clayton & Fulton Counties, GA)

## Quick Start
```bash
cd /home/user/waste-lead-scanner
pip install -r requirements.txt
cp config/config.example.yaml config/config.yaml  # Add API key
python -m src.main --client arrow
streamlit run src/dashboard/app.py
```

## Stack
- Shovels.ai API (permits + contractors)
- Python 3.12+
- BigQuery (storage)
- Streamlit (dashboard)
- GCP (profitscout-fida8)
