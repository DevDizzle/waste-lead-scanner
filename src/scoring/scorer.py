import datetime
import math
import logging

logger = logging.getLogger(__name__)

class LeadScorer:
    TAG_SCORES = {
        'new_construction': 10,
        'demolition': 10,
        'addition': 7,
        'remodel': 6,
        'roofing': 5,
        'kitchen': 4,
        'bathroom': 4,
        'grading': 8,
        'pool_and_hot_tub': 6,
        'plumbing': 2,
        'electrical': 2,
        'hvac': 2
    }
    
    PROPERTY_TYPE_SCORES = {
        'commercial': 10,
        'industrial': 10,
        'residential': 5
    }
    
    @staticmethod
    def score_waste_volume(tags):
        if not tags:
            return 0
        scores = [LeadScorer.TAG_SCORES.get(tag, 0) for tag in tags]
        return max(scores) if scores else 0

    @staticmethod
    def score_property_type(prop_type):
        if not prop_type:
            return 3
        return LeadScorer.PROPERTY_TYPE_SCORES.get(prop_type.lower(), 3)

    @staticmethod
    def score_recency(file_date_str):
        if not file_date_str:
            return 0
        try:
            # Handle YYYY-MM-DD format commonly returned by APIs
            file_date = datetime.datetime.strptime(file_date_str[:10], "%Y-%m-%d").date()
            today = datetime.date.today()
            days_ago = (today - file_date).days
            
            if days_ago <= 0: return 10
            elif days_ago <= 7: return 7 + (10 - 7) * (7 - days_ago) / 7  # interpolate
            elif days_ago <= 14: return 5 + (7 - 5) * (14 - days_ago) / 7
            elif days_ago <= 21: return 3 + (5 - 3) * (21 - days_ago) / 7
            elif days_ago <= 30: return 1 + (3 - 1) * (30 - days_ago) / 9
            else: return 0
        except Exception as e:
            logger.warning(f"Error parsing date {file_date_str}: {e}")
            return 0

    @staticmethod
    def score_job_value(job_value):
        if job_value is None or str(job_value).strip() == '':
            return 5
            
        try:
            val = float(job_value)
            if val <= 0:
                return 5 # or 1? Prompt says: "If 0 or null, default to 5. If job_value > 0: scale 1-10 based on value ($0=1, $50K=5, $200K+=10)"
                # Let's clarify: If 0 or null, default 5. If > 0, interpolate. 
                # Wait, prompt says: "If job_value > 0: scale 1-10 based on value ($0=1, $50K=5, $200K+=10). If 0 or null, default to 5."
            
            if val == 0:
                return 5
            elif val < 50000:
                return 1 + (5 - 1) * (val / 50000)
            elif val < 200000:
                return 5 + (10 - 5) * ((val - 50000) / 150000)
            else:
                return 10
        except (ValueError, TypeError):
            return 5

    @classmethod
    def score_lead(cls, permit):
        score_breakdown = {}
        
        vol_score = cls.score_waste_volume(permit.get('tags', []) or permit.get('permit_tags', []))
        prop_score = cls.score_property_type(permit.get('property_type'))
        rec_score = cls.score_recency(permit.get('file_date'))
        val_score = cls.score_job_value(permit.get('job_value'))
        
        score_breakdown['waste_volume'] = vol_score
        score_breakdown['property_type'] = prop_score
        score_breakdown['recency'] = rec_score
        score_breakdown['job_value'] = val_score
        
        final_score = (vol_score * 0.40) + (prop_score * 0.25) + (rec_score * 0.20) + (val_score * 0.15)
        
        permit_copy = permit.copy()
        permit_copy['score'] = round(final_score, 1)
        permit_copy['score_breakdown'] = score_breakdown
        return permit_copy
        
    @classmethod
    def score_permits(cls, permits):
        logger.info(f"Scoring {len(permits)} permits...")
        return [cls.score_lead(p) for p in permits]
