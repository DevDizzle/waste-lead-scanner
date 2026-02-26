import argparse
import yaml
import logging
import datetime
from src.scanner.shovels import ShovelsScanner
from src.enrichment.contractors import ContractorEnricher
from src.scoring.scorer import LeadScorer
from src.storage.bq import BigQueryStorage

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_path="config/config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description="Waste Lead Scanner MVP")
    parser.add_argument("--client", required=True, help="Client name to run (e.g. arrow)")
    parser.add_argument("--skip-enrich", action="store_true", help="Skip contractor enrichment to save API quota")
    parser.add_argument("--limit", type=int, help="Limit number of permits processed")
    args = parser.parse_args()

    config = load_config()
    
    if args.client not in config.get('clients', {}):
        logger.error(f"Client '{args.client}' not found in config.")
        return

    client_config = config['clients'][args.client]
    logger.info(f"Starting run for client: {client_config['name']}")

    # Dates: last 90 days to ensure data for MVP
    today = datetime.date.today()
    permit_to = today.isoformat()
    permit_from = (today - datetime.timedelta(days=90)).isoformat()

    # 1. Scan Permits
    scanner = ShovelsScanner(config)
    permits, scan_api_calls = scanner.run_scan(client_config, permit_from, permit_to, limit=args.limit)
    
    # 2. Storage Init
    bq_storage = BigQueryStorage(config)
    
    # Save raw permits
    bq_storage.save_raw_permits(permits)

    # 3. Enrichment
    enrich_api_calls = 0
    if args.skip_enrich:
        logger.info("Skipping enrichment as requested.")
        enriched_permits = permits
        for p in enriched_permits:
            if 'contractor_name' not in p:
                p['contractor_name'] = None
                p['contractor_employees'] = []
    else:
        enricher = ContractorEnricher(config)
        enriched_permits, enrich_api_calls, new_contractors = enricher.enrich_permits(permits)
        bq_storage.save_new_contractors(new_contractors)

    # 4. Scoring
    scored_permits = LeadScorer.score_permits(enriched_permits)
    
    # 5. Save Scored Leads
    bq_storage.save_scored_leads(scored_permits)
    
    # Output summary
    total_api_calls = scan_api_calls + enrich_api_calls
    logger.info("="*50)
    logger.info("PIPELINE SUMMARY")
    logger.info(f"Total API Calls Used: {total_api_calls} (Scan: {scan_api_calls}, Enrich: {enrich_api_calls})")
    logger.info(f"Total Permits Scanned: {len(permits)}")
    logger.info(f"Total Permits Scored & Saved: {len(scored_permits)}")
    
    # County breakdown
    county_counts = {}
    for p in scored_permits:
        c = p.get('county_name', 'Unknown')
        county_counts[c] = county_counts.get(c, 0) + 1
    logger.info(f"Permits by County: {county_counts}")
    
    # Top 5 leads
    sorted_leads = sorted(scored_permits, key=lambda x: x.get('score', 0), reverse=True)
    logger.info("Top 5 Leads:")
    for i, lead in enumerate(sorted_leads[:5]):
        logger.info(f"  {i+1}. Score {lead.get('score')} - {lead.get('address')} ({lead.get('permit_type')} - {lead.get('permit_tags')})")

if __name__ == "__main__":
    main()