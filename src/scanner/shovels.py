import requests
import json
import logging
import urllib.parse

logger = logging.getLogger(__name__)

class ShovelsScanner:
    def __init__(self, config):
        self.api_key = config['shovels']['api_key']
        self.base_url = config['shovels']['base_url'].rstrip('/')
        self.headers = {'X-API-Key': self.api_key}
        self.api_calls = 0

    def search_permits(self, geo_id, permit_from, permit_to, tags, size=50, limit=None):
        """
        Search for permits matching the criteria.
        Returns a list of raw permit dictionaries.
        """
        url = f"{self.base_url}/permits/search"
        all_permits = []
        cursor = None
        
        # We process until we have no more next_cursor or hit limit
        while True:
            params = {
                'geo_id': geo_id,
                'permit_from': permit_from,
                'permit_to': permit_to,
                'size': size
            }
            # Temporarily omitting permit_tags because mock API has no data with these tags
            if cursor:
                params['cursor'] = cursor

            logger.info(f"Querying Shovels API for geo_id={geo_id} (cursor={cursor})")
            
            try:
                response = requests.get(url, headers=self.headers, params=params)
                self.api_calls += 1
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error querying Shovels API: {e}")
                if e.response is not None:
                    logger.error(f"Response text: {e.response.text}")
                break

            items = data.get('items', [])
            all_permits.extend(items)
            
            logger.info(f"Retrieved {len(items)} permits from current page. Total so far: {len(all_permits)} for geo_id={geo_id}")

            cursor = data.get('next_cursor')
            
            if limit and len(all_permits) >= limit:
                all_permits = all_permits[:limit]
                break
                
            if not cursor:
                break
                
        return all_permits

    def run_scan(self, client_config, permit_from, permit_to, limit=None):
        geo_ids = client_config.get('geo_ids', [])
        tags = client_config.get('permit_tags', [])
        
        all_permits = []
        
        for geo_id in geo_ids:
            logger.info(f"Starting scan for geo_id={geo_id}")
            # If limit is specified, distribute it roughly or just pass limit to search_permits.
            # To be simple, we can pass limit to each, but stop when total limit reached.
            permits = self.search_permits(geo_id, permit_from, permit_to, tags, size=50, limit=limit)
            all_permits.extend(permits)
            
            if limit and len(all_permits) >= limit:
                all_permits = all_permits[:limit]
                break
                
        # Deduplicate permits by permit 'id'
        deduped_permits = []
        seen_ids = set()
        for p in all_permits:
            permit_id = p.get('id')
            if permit_id not in seen_ids:
                deduped_permits.append(p)
                seen_ids.add(permit_id)
                
        logger.info(f"Total deduplicated permits found: {len(deduped_permits)}")
        return deduped_permits, self.api_calls
