"""
Groq-based Category and Severity Predictor
Uses another.csv as training examples for few-shot learning
Optimized to minimize API calls with caching
"""
import os
import logging
import json
import hashlib
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

class GeminiPredictor:
    """
    Predicts category and severity for incident descriptions
    Uses Groq LLM with few-shot learning and examples from another.csv
    (Class name kept for backward compatibility)
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.groq_client = None
        self.training_examples = []
        self.cache = {}  # Cache predictions to avoid redundant API calls
        self.cache_ttl = timedelta(hours=24)  # Cache for 24 hours
        
        # Initialize Groq
        if not GROQ_AVAILABLE:
            raise ImportError("Groq not available. Install with: pip install groq")
        
        groq_api_key = os.getenv('GROQ_API_KEY')
        if not groq_api_key:
            raise ValueError("GROQ_API_KEY not found in environment")
        
        try:
            self.groq_client = Groq(api_key=groq_api_key)
            self.logger.info("Groq predictor initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Groq: {e}")
            raise
        
        # Load training examples from another.csv
        self._load_training_examples()
    
    def _load_training_examples(self):
        """Load training examples from another.csv"""
        try:
            csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'another.csv')
            
            if not os.path.exists(csv_path):
                self.logger.warning(f"another.csv not found at {csv_path}")
                return
            
            df = pd.read_csv(csv_path)
            
            # Validate required columns
            required_cols = ['Category', 'Severity', 'Description']
            if not all(col in df.columns for col in required_cols):
                self.logger.error(f"another.csv missing required columns: {required_cols}")
                return
            
            # Sample diverse examples (max 20 for few-shot learning)
            # Get examples from each category and severity
            examples = []
            
            for category in df['Category'].unique():
                for severity in df['Severity'].unique():
                    subset = df[(df['Category'] == category) & (df['Severity'] == severity)]
                    if not subset.empty:
                        # Take 1 example from each category-severity combination
                        examples.append(subset.iloc[0])
            
            # If we have more than 20, sample randomly
            if len(examples) > 20:
                examples = pd.DataFrame(examples).sample(n=20, random_state=42).to_dict('records')
            else:
                examples = [ex.to_dict() if hasattr(ex, 'to_dict') else ex for ex in examples]
            
            self.training_examples = examples
            self.logger.info(f"Loaded {len(self.training_examples)} training examples from another.csv")
            
        except Exception as e:
            self.logger.error(f"Error loading training examples: {e}")
    
    def _get_cache_key(self, description):
        """Generate cache key from description"""
        return hashlib.md5(description.lower().strip().encode()).hexdigest()
    
    def _get_from_cache(self, description):
        """Get prediction from cache if available and not expired"""
        cache_key = self._get_cache_key(description)
        
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            if datetime.now() - cached_data['timestamp'] < self.cache_ttl:
                self.logger.info(f"[CACHE HIT] Using cached prediction for: {description[:50]}...")
                return cached_data['prediction']
            else:
                # Expired, remove from cache
                del self.cache[cache_key]
        
        return None
    
    def _save_to_cache(self, description, prediction):
        """Save prediction to cache"""
        cache_key = self._get_cache_key(description)
        self.cache[cache_key] = {
            'prediction': prediction,
            'timestamp': datetime.now()
        }
    
    def predict(self, description):
        """
        Predict category and severity for a given description
        
        Args:
            description: Incident description text
            
        Returns:
            dict: {
                'category': str,
                'severity': str,
                'confidence': float,
                'reasoning': str
            }
        """
        # Check cache first
        cached = self._get_from_cache(description)
        if cached:
            return cached
        
        try:
            # Build few-shot prompt with training examples
            examples_text = ""
            if self.training_examples:
                examples_text = "\n\nTraining Examples:\n"
                for i, ex in enumerate(self.training_examples[:15], 1):  # Use top 15 examples
                    examples_text += f"{i}. Description: \"{ex['Description']}\"\n"
                    examples_text += f"   Category: {ex['Category']}, Severity: {ex['Severity']}\n\n"
            
            prompt = f"""You are an ITSM incident classification expert. Based on the training examples below, classify the given incident description into a Category and Severity level.

Categories: Database, API, Frontend, Infrastructure, Authentication, Payment, Network, Application, Security, Email, Storage, Monitoring, Configuration, Deployment

Severity Levels: Critical, High, Medium, Low
- Critical: Complete service outage, data loss, security breach
- High: Major functionality broken, significant user impact
- Medium: Partial functionality affected, workarounds available
- Low: Minor issues, cosmetic problems, minimal impact
{examples_text}
Now classify this incident:
Description: "{description}"

Respond in JSON format:
{{
  "category": "one of the categories above",
  "severity": "Critical|High|Medium|Low",
  "confidence": 0.0-1.0,
  "reasoning": "brief explanation of why this category and severity"
}}
"""
            
            # Use Groq for prediction
            completion = self.groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",  # Fast and accurate
                messages=[
                    {"role": "system", "content": "You are an ITSM incident classification expert. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,  # Low temperature for consistent classification
                max_tokens=300
            )
            
            result_text = completion.choices[0].message.content.strip()
            
            # Parse JSON response - remove markdown code blocks if present
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(result_text)
            
            # Validate and normalize
            result['category'] = result.get('category', 'Application')
            result['severity'] = result.get('severity', 'Medium')
            result['confidence'] = float(result.get('confidence', 0.7))
            result['reasoning'] = result.get('reasoning', 'Classification based on description')
            
            self.logger.info(f"[GROQ PREDICT] {description[:50]}... -> {result['category']}/{result['severity']} ({result['confidence']:.2f})")
            
            # Cache the result
            self._save_to_cache(description, result)
            
            return result
            
        except Exception as e:
            error_str = str(e)
            
            # Handle rate limit errors
            if '429' in error_str or 'rate_limit' in error_str.lower():
                self.logger.warning(f"[RATE LIMIT] Groq API rate limit hit - waiting 5 seconds...")
                time.sleep(5)
                # Return default classification on rate limit
                return {
                    'category': 'Application',
                    'severity': 'Medium',
                    'confidence': 0.3,
                    'reasoning': 'Rate limit hit, using default classification'
                }
            
            self.logger.error(f"Error in Groq prediction: {e}")
            # Return default classification
            return {
                'category': 'Application',
                'severity': 'Medium',
                'confidence': 0.3,
                'reasoning': f'Error during classification: {str(e)}'
            }
    
    def clear_cache(self):
        """Clear prediction cache"""
        self.cache = {}
        self.logger.info("Prediction cache cleared")
    
    def get_cache_stats(self):
        """Get cache statistics"""
        return {
            'size': len(self.cache),
            'entries': list(self.cache.keys())
        }


# Singleton instance
_predictor_instance = None

def get_predictor():
    """Get or create singleton predictor instance"""
    global _predictor_instance
    if _predictor_instance is None:
        _predictor_instance = GeminiPredictor()
    return _predictor_instance
