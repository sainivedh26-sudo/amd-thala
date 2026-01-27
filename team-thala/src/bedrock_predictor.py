"""
Bedrock-based Category and Severity Predictor
Drop-in replacement for gemini_predictor.get_predictor()
Uses AWS Bedrock Llama 3.3 70B via Converse API
"""
import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

try:
    import boto3
    from botocore.config import Config
    BEDROCK_AVAILABLE = True
except Exception:
    BEDROCK_AVAILABLE = False


class BedrockPredictor:
    """
    Predicts category and severity for incident descriptions using Bedrock
    Maintains same public API as GeminiPredictor
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.cache = {}
        self.cache_ttl = timedelta(hours=24)

        if not BEDROCK_AVAILABLE:
            raise ImportError("boto3/bedrock not available. Install with: pip install boto3 botocore")

        region = os.getenv('AWS_REGION', 'us-east-2')
        self.model_id = os.getenv('BEDROCK_LLAMA_MODEL_ID', 'us.meta.llama3-3-70b-instruct-v1:0')  # Use inference profile ID with 'us.' prefix

        # Configure retries for Bedrock runtime
        cfg = Config(retries={"max_attempts": 3, "mode": "standard"})
        self.client = boto3.client('bedrock-runtime', region_name=region, config=cfg)
        self.logger.info(f"Bedrock predictor initialized in {region} with model {self.model_id}")

    def _cache_key(self, description: str) -> str:
        return hashlib.md5(description.lower().strip().encode()).hexdigest()

    def _from_cache(self, description):
        key = self._cache_key(description)
        entry = self.cache.get(key)
        if entry and datetime.now() - entry['ts'] < self.cache_ttl:
            self.logger.info(f"[CACHE HIT] Using cached prediction for: {description[:50]}...")
            return entry['value']
        return None

    def _save_cache(self, description, value):
        key = self._cache_key(description)
        self.cache[key] = {"value": value, "ts": datetime.now()}

    def predict(self, description: str) -> dict:
        cached = self._from_cache(description)
        if cached:
            return cached

        system_prompt = (
            "You are an ITSM incident classification expert. "
            "Classify the incident into category and severity. Always return strict JSON."
        )

        user_prompt = f"""
Classify the incident.

Categories: Database, API, Frontend, Infrastructure, Authentication, Payment, Network, Application, Security, Email, Storage, Monitoring, Configuration, Deployment

Severity Levels: Critical, High, Medium, Low

Description: "{description}"

Respond JSON only:
{{
  "category": "one category above",
  "severity": "Critical|High|Medium|Low",
  "confidence": 0.0-1.0,
  "reasoning": "brief explanation"
}}
"""

        try:
            resp = self.client.converse(
                modelId=self.model_id,
                system=[{"text": system_prompt}],
                messages=[
                    {"role": "user", "content": [{"text": user_prompt}]},
                ],
                inferenceConfig={
                    "maxTokens": 400,
                    "temperature": 0.2,
                    "topP": 0.9,
                },
            )

            content = resp['output']['message']['content']
            text_parts = [c.get('text') for c in content if 'text' in c]
            result_text = ("\n".join([t for t in text_parts if t])).strip()

            # Strip code fences if any
            if '```json' in result_text:
                result_text = result_text.split('```json')[1].split('```')[0].strip()
            elif '```' in result_text:
                result_text = result_text.split('```')[1].split('```')[0].strip()

            parsed = json.loads(result_text)
            parsed['category'] = parsed.get('category', 'Application')
            parsed['severity'] = parsed.get('severity', 'Medium')
            parsed['confidence'] = float(parsed.get('confidence', 0.7))
            parsed['reasoning'] = parsed.get('reasoning', 'Classification based on description')

            self._save_cache(description, parsed)
            return parsed
        except Exception as e:
            self.logger.error(f"Bedrock prediction error: {e}")
            return {
                'category': 'Application',
                'severity': 'Medium',
                'confidence': 0.3,
                'reasoning': f'Error during classification: {str(e)}'
            }

    def clear_cache(self):
        self.cache = {}

    def get_cache_stats(self):
        return {'size': len(self.cache), 'entries': list(self.cache.keys())}


_predictor_instance = None

def get_predictor():
    global _predictor_instance
    if _predictor_instance is None:
        _predictor_instance = BedrockPredictor()
    return _predictor_instance



