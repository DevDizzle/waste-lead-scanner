import requests
import json
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

class ContractorEnricher:
    def __init__(self, config):
        self.api_key = config['shovels']['api_key']
        self.base_url = config['shovels']['base_url'].rstrip('/')
        self.headers = {'X-API-Key': self.api_key}
        self.cache = {}
        self.api_calls = 0
        self.new_contractors = []
        
        # Try to load existing cache from BigQuery
        self.project_id = config['gcp']['project_id']
        self.dataset = config['gcp']['dataset']
        self.bq_client = bigquery.Client(project=self.project_id)
        self._load_cache_from_bq()

    def _load_cache_from_bq(self):
        table_id = f"{self.project_id}.{self.dataset}.contractors"
        query = f"SELECT contractor_id, name, employees FROM `{table_id}`"
        try:
            query_job = self.bq_client.query(query)
            for row in query_job:
                employees = row.employees
                if isinstance(employees, str):
                    try:
                        employees = json.loads(employees)
                    except:
                        employees = []
                self.cache[row.contractor_id] = {
                    'name': row.name,
                    'employees': employees
                }
            logger.info(f"Loaded {len(self.cache)} contractors from BigQuery cache.")
        except NotFound:
            logger.info(f"Table {table_id} not found. Starting with empty cache.")
        except Exception as e:
            logger.error(f"Error loading cache from BQ: {e}. Starting with empty cache.")

    def get_contractor(self, contractor_id):
        if not contractor_id:
            return None
            
        if contractor_id in self.cache:
            return self.cache[contractor_id]
            
        # Fetch from API
        url = f"{self.base_url}/contractors/{contractor_id}"
        contractor_data = {'name': None, 'employees': []}
        
        try:
            logger.info(f"Fetching contractor API for {contractor_id}")
            response = requests.get(url, headers=self.headers)
            self.api_calls += 1
            if response.status_code == 200:
                data = response.json()
                contractor_data['name'] = data.get('name')
            else:
                logger.warning(f"Failed to fetch contractor {contractor_id}: {response.status_code}")
                
            # Try getting employees
            emp_url = f"{self.base_url}/contractors/{contractor_id}/employees"
            emp_response = requests.get(emp_url, headers=self.headers)
            self.api_calls += 1
            if emp_response.status_code == 200:
                emp_data = emp_response.json()
                contractor_data['employees'] = emp_data.get('items', [])
            else:
                logger.debug(f"Failed to fetch employees for {contractor_id}: {emp_response.status_code}")
                
        except Exception as e:
            logger.error(f"Error fetching contractor {contractor_id}: {e}")
            
        self.cache[contractor_id] = contractor_data
        self.new_contractors.append({
            'contractor_id': contractor_id,
            'name': contractor_data['name'],
            'employees': json.dumps(contractor_data['employees'])
        })
        return contractor_data

    def enrich_permits(self, permits):
        logger.info(f"Enriching {len(permits)} permits with contractor data...")
        enriched = []
        for p in permits:
            permit_copy = p.copy()
            c_id = permit_copy.get('contractor_id')
            if c_id:
                c_data = self.get_contractor(c_id)
                if c_data:
                    permit_copy['contractor_name'] = c_data.get('name')
                    permit_copy['contractor_employees'] = c_data.get('employees', [])
                else:
                    permit_copy['contractor_name'] = None
                    permit_copy['contractor_employees'] = []
            else:
                permit_copy['contractor_name'] = None
                permit_copy['contractor_employees'] = []
            enriched.append(permit_copy)
        
        logger.info(f"Enrichment complete. Used {self.api_calls} API calls.")
        return enriched, self.api_calls, self.new_contractors
