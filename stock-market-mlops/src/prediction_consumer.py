import json
import joblib
import logging
from kafka import KafkaConsumer
from datetime import datetime

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_FEATURES_TOPIC,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PredictionConsumer:
    """Consumes engineered features from Kafka and produces real-time predictions."""
    
    def __init__(self, model_path="models/model.pkl"):
        """Initialize prediction consumer with trained model."""
        try:
            self.model = joblib.load(model_path)
            logger.info(f"✅ Model loaded from {model_path}")
        except FileNotFoundError:
            logger.error(f"❌ Model not found at {model_path}")
            logger.error("   Please run: python src/train_model.py")
            raise
        
        self.feature_columns = ['MA_10', 'MA_50', 'Return', 'Lag_1', 'Lag_2', 'Volatility']
        self.predictions = {}
    
    def build_consumer(self):
        """Build Kafka consumer for features topic."""
        return KafkaConsumer(
            KAFKA_FEATURES_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="stock-prediction",
            client_id=KAFKA_CLIENT_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda v: v.decode("utf-8") if v else None,
        )
    
    def predict(self, features):
        """
        Make prediction on engineered features.
        
        Args:
            features (dict): Dictionary containing feature values
            
        Returns:
            float: Predicted next-day closing price
        """
        # Extract features in correct order
        feature_values = [features.get(col) for col in self.feature_columns]
        
        # Check for missing features
        if None in feature_values:
            missing = [self.feature_columns[i] for i, v in enumerate(feature_values) if v is None]
            logger.warning(f"⚠️  Missing features: {missing}")
            return None
        
        # Make prediction
        prediction = self.model.predict([feature_values])[0]
        return prediction
    
    def run(self):
        """Main consumer loop."""
        consumer = self.build_consumer()
        logger.info("🚀 Starting prediction consumer...")
        logger.info(f"📡 Listening on topic: {KAFKA_FEATURES_TOPIC}")
        
        try:
            for message in consumer:
                payload = message.value
                symbol = payload.get("symbol")
                timestamp = payload.get("timestamp")
                
                if not symbol:
                    continue
                
                # Extract features
                features = {
                    'MA_10': payload.get('MA_10'),
                    'MA_50': payload.get('MA_50'),
                    'Return': payload.get('Return'),
                    'Lag_1': payload.get('Lag_1'),
                    'Lag_2': payload.get('Lag_2'),
                    'Volatility': payload.get('Volatility'),
                }
                
                # Make prediction
                prediction = self.predict(features)
                
                if prediction is not None:
                    self.predictions[symbol] = {
                        'timestamp': timestamp,
                        'prediction': prediction,
                        'current_price': payload.get('close'),
                        'features': features
                    }
                    
                    # Log prediction
                    current_price = payload.get('close', 'N/A')
                    logger.info(
                        f"💰 {symbol:6} | Time: {timestamp:20} | "
                        f"Current: ${current_price:8.2f} | Predicted: ${prediction:8.2f}"
                    )
        
        except KeyboardInterrupt:
            logger.info("⏹️  Prediction consumer stopped by user")
        except Exception as e:
            logger.error(f"❌ Error in prediction consumer: {e}", exc_info=True)
        finally:
            consumer.close()


if __name__ == "__main__":
    consumer = PredictionConsumer()
    consumer.run()
