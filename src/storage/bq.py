import json
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import datetime

logger = logging.getLogger(__name__)

class BigQueryStorage:
    def __init__(self, config):
        self.project_id = config['gcp']['project_id']
        self.dataset_id = config['gcp']['dataset']
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_ref = f"{self.project_id}.{self.dataset_id}"
        self._ensure_dataset()

    def _ensure_dataset(self):
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_ref} already exists.")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {self.dataset_ref}")

    def parse_permit_fields(self, p):
        # Extract common fields safely from Shovels API response
        addr_obj = p.get('address') or {}
        address_str, city, county, zipcode, lat, lng = None, None, None, None, None, None
        if isinstance(addr_obj, dict):
            address_str = f"{addr_obj.get('street_no', '')} {addr_obj.get('street', '')}".strip()
            city = addr_obj.get('city')
            county = addr_obj.get('county')
            zipcode = addr_obj.get('zip_code')
            latlng = addr_obj.get('latlng')
            if latlng and len(latlng) == 2:
                lat, lng = latlng[0], latlng[1]
        elif isinstance(addr_obj, str):
            address_str = addr_obj

        job_val = p.get('job_value')
        if job_val == '' or job_val is None:
            job_val = None
        else:
            try:
                job_val = float(job_val)
            except:
                job_val = None

        file_date = p.get('file_date')
        if file_date:
            file_date = file_date[:10]

        tags = p.get('tags') or p.get('permit_tags') or []
        permit_type = p.get('type') or p.get('permit_type')

        return {
            "address": address_str,
            "city": city,
            "county": county,
            "zip": zipcode,
            "lat": float(lat) if lat is not None else None,
            "lng": float(lng) if lng is not None else None,
            "job_val": job_val,
            "file_date": file_date,
            "tags": tags,
            "permit_type": permit_type
        }

    def save_raw_permits(self, permits):
        table_id = f"{self.dataset_ref}.raw_permits"
        schema = [
            bigquery.SchemaField("permit_id", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("county", "STRING"),
            bigquery.SchemaField("zip", "STRING"),
            bigquery.SchemaField("lat", "FLOAT"),
            bigquery.SchemaField("lng", "FLOAT"),
            bigquery.SchemaField("contractor_id", "STRING"),
            bigquery.SchemaField("permit_type", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField("file_date", "DATE"),
            bigquery.SchemaField("job_value", "FLOAT"),
            bigquery.SchemaField("property_type", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("permit_data", "JSON")
        ]
        
        rows = []
        for p in permits:
            try:
                fields = self.parse_permit_fields(p)
                    
                rows.append({
                    "permit_id": p.get('id'),
                    "address": fields['address'],
                    "city": fields['city'],
                    "county": fields['county'],
                    "zip": fields['zip'],
                    "lat": fields['lat'],
                    "lng": fields['lng'],
                    "contractor_id": p.get('contractor_id'),
                    "permit_type": fields['permit_type'],
                    "tags": fields['tags'],
                    "file_date": fields['file_date'],
                    "job_value": fields['job_val'],
                    "property_type": p.get('property_type'),
                    "status": p.get('status'),
                    "permit_data": json.dumps(p)
                })
            except Exception as e:
                logger.error(f"Error parsing raw permit {p.get('id')}: {e}")
        
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )
        
        if rows:
            logger.info(f"Saving {len(rows)} raw permits to {table_id}...")
            job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
            job.result()
            logger.info("Saved raw permits.")

    def save_scored_leads(self, scored_permits):
        table_id = f"{self.dataset_ref}.scored_leads"
        schema = [
            bigquery.SchemaField("permit_id", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("county", "STRING"),
            bigquery.SchemaField("zip", "STRING"),
            bigquery.SchemaField("lat", "FLOAT"),
            bigquery.SchemaField("lng", "FLOAT"),
            bigquery.SchemaField("contractor_id", "STRING"),
            bigquery.SchemaField("permit_type", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField("file_date", "DATE"),
            bigquery.SchemaField("job_value", "FLOAT"),
            bigquery.SchemaField("property_type", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("permit_data", "JSON"),
            bigquery.SchemaField("score", "FLOAT"),
            bigquery.SchemaField("score_breakdown", "JSON"),
            bigquery.SchemaField("contractor_name", "STRING"),
            bigquery.SchemaField("contractor_employees", "JSON")
        ]
        
        rows = []
        for p in scored_permits:
            try:
                fields = self.parse_permit_fields(p)

                # Save a copy without custom fields added
                clean_p = {k: v for k, v in p.items() if k not in ['score', 'score_breakdown', 'contractor_name', 'contractor_employees', 'parsed_fields']}

                rows.append({
                    "permit_id": p.get('id'),
                    "address": fields['address'],
                    "city": fields['city'],
                    "county": fields['county'],
                    "zip": fields['zip'],
                    "lat": fields['lat'],
                    "lng": fields['lng'],
                    "contractor_id": p.get('contractor_id'),
                    "permit_type": fields['permit_type'],
                    "tags": fields['tags'],
                    "file_date": fields['file_date'],
                    "job_value": fields['job_val'],
                    "property_type": p.get('property_type'),
                    "status": p.get('status'),
                    "permit_data": json.dumps(clean_p),
                    "score": p.get('score'),
                    "score_breakdown": json.dumps(p.get('score_breakdown', {})),
                    "contractor_name": p.get('contractor_name'),
                    "contractor_employees": json.dumps(p.get('contractor_employees', []))
                })
            except Exception as e:
                logger.error(f"Error parsing scored permit {p.get('id')}: {e}")
        
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )
        
        if rows:
            logger.info(f"Saving {len(rows)} scored leads to {table_id}...")
            job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
            job.result()
            logger.info("Saved scored leads.")

    def save_new_contractors(self, new_contractors):
        if not new_contractors:
            return
            
        table_id = f"{self.dataset_ref}.contractors"
        schema = [
            bigquery.SchemaField("contractor_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("employees", "JSON"),
            bigquery.SchemaField("last_fetched", "TIMESTAMP")
        ]
        
        rows = []
        now = datetime.datetime.utcnow().isoformat()
        for c in new_contractors:
            rows.append({
                "contractor_id": c['contractor_id'],
                "name": c['name'],
                "employees": c['employees'],
                "last_fetched": now
            })
            
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
        )
        
        logger.info(f"Saving {len(rows)} new contractors to {table_id}...")
        try:
            job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
            job.result()
            logger.info("Saved new contractors.")
        except NotFound:
            # Table might not exist yet, CREATE it
            logger.info("Contractors table not found, creating it...")
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
            job.result()
            logger.info("Created and saved new contractors.")
        except Exception as e:
            logger.error(f"Failed to save contractors: {e}")