import json
import logging
from collections import defaultdict, deque
from kafka import KafkaConsumer
import numpy as np

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


class DriftMonitor:
    """
    Monitors data drift in streaming features.
    
    Compares live feature distributions against training distribution baseline.
    Alerts when drift is detected beyond thresholds.
    """
    
    def __init__(self, window_size=100, drift_threshold=0.3):
        """
        Initialize drift monitor.
        
        Args:
            window_size (int): Number of recent samples to track for rolling stats
            drift_threshold (float): Standard deviations threshold for drift alert
        """
        self.window_size = window_size
        self.drift_threshold = drift_threshold
        
        # Track rolling statistics per symbol
        self.price_buffers = defaultdict(lambda: deque(maxlen=window_size))
        self.return_buffers = defaultdict(lambda: deque(maxlen=window_size))
        self.volatility_buffers = defaultdict(lambda: deque(maxlen=window_size))
        
        # Training distribution baselines (from historical data)
        # These would typically be calculated from training data
        self.baselines = {
            'price': {'mean': None, 'std': None},
            'return': {'mean': None, 'std': None},
            'volatility': {'mean': None, 'std': None},
        }
        
        self.drift_alerts = defaultdict(list)
    
    def set_baseline(self, metric_name, mean, std):
        """Set baseline statistics for a metric."""
        if metric_name in self.baselines:
            self.baselines[metric_name] = {'mean': mean, 'std': std}
            logger.info(f"📊 Baseline set for {metric_name}: μ={mean:.4f}, σ={std:.4f}")
    
    def build_consumer(self):
        """Build Kafka consumer for features topic."""
        return KafkaConsumer(
            KAFKA_FEATURES_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="stock-drift-monitor",
            client_id=KAFKA_CLIENT_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda v: v.decode("utf-8") if v else None,
        )
    
    def check_drift(self, metric_name, value, symbol):
        """
        Check if a metric value indicates drift.
        
        Args:
            metric_name (str): Name of the metric ('price', 'return', 'volatility')
            value (float): Current metric value
            symbol (str): Stock symbol
            
        Returns:
            bool: True if drift detected, False otherwise
        """
        baseline = self.baselines[metric_name]
        
        if baseline['mean'] is None or baseline['std'] is None:
            return False
        
        # Calculate z-score
        z_score = abs((value - baseline['mean']) / (baseline['std'] + 1e-9))
        
        if z_score > self.drift_threshold:
            return True
        
        return False
    
    def get_rolling_stats(self, metric_name, symbol):
        """Get rolling mean and std for a metric."""
        if metric_name == 'price':
            buffer = self.price_buffers[symbol]
        elif metric_name == 'return':
            buffer = self.return_buffers[symbol]
        elif metric_name == 'volatility':
            buffer = self.volatility_buffers[symbol]
        else:
            return None, None
        
        if len(buffer) == 0:
            return None, None
        
        return np.mean(buffer), np.std(buffer)
    
    def run(self):
        """Main monitoring loop."""
        consumer = self.build_consumer()
        
        # Initialize baselines from typical stock market statistics
        # Price: assume $150 mean with $15 std
        self.set_baseline('price', mean=150.0, std=15.0)
        # Return: assume 0% mean with 0.02 (2%) std
        self.set_baseline('return', mean=0.0, std=0.02)
        # Volatility: assume 0.015 (1.5%) mean with 0.005 std
        self.set_baseline('volatility', mean=0.015, std=0.005)
        
        logger.info("🚀 Starting drift monitor...")
        logger.info(f"📡 Listening on topic: {KAFKA_FEATURES_TOPIC}")
        logger.info(f"⚙️  Window size: {self.window_size}, Drift threshold: {self.drift_threshold}σ")
        
        try:
            message_count = 0
            
            for message in consumer:
                payload = message.value
                symbol = payload.get("symbol")
                timestamp = payload.get("timestamp")
                
                if not symbol:
                    continue
                
                message_count += 1
                
                # Extract metrics
                close_price = payload.get('close')
                return_val = payload.get('Return')
                volatility = payload.get('Volatility')
                
                # Track rolling values
                if close_price is not None:
                    self.price_buffers[symbol].append(close_price)
                if return_val is not None:
                    self.return_buffers[symbol].append(return_val)
                if volatility is not None:
                    self.volatility_buffers[symbol].append(volatility)
                
                # Check for drift every 10 messages (reduce noise)
                if message_count % 10 == 0:
                    drifts_detected = []
                    
                    # Check price drift
                    if close_price is not None and self.check_drift('price', close_price, symbol):
                        drifts_detected.append(f"Price drift (${close_price:.2f})")
                    
                    # Check return drift
                    if return_val is not None and self.check_drift('return', return_val, symbol):
                        drifts_detected.append(f"Return drift ({return_val:.4f})")
                    
                    # Check volatility drift
                    if volatility is not None and self.check_drift('volatility', volatility, symbol):
                        drifts_detected.append(f"Volatility drift ({volatility:.4f})")
                    
                    # Log rolling statistics
                    price_mean, price_std = self.get_rolling_stats('price', symbol)
                    ret_mean, ret_std = self.get_rolling_stats('return', symbol)
                    vol_mean, vol_std = self.get_rolling_stats('volatility', symbol)
                    
                    # Alert on drift
                    if drifts_detected:
                        logger.warning(f"⚠️  DRIFT ALERT for {symbol}: {', '.join(drifts_detected)}")
                        self.drift_alerts[symbol].append({
                            'timestamp': timestamp,
                            'alerts': drifts_detected
                        })
                    else:
                        # Normal operation log
                        logger.info(
                            f"✅ {symbol:6} | Time: {timestamp:20} | "
                            f"Price: μ={price_mean:7.2f}, σ={price_std:5.2f} | "
                            f"Return: μ={ret_mean:7.5f}, σ={ret_std:7.5f}"
                        )
        
        except KeyboardInterrupt:
            logger.info("⏹️  Drift monitor stopped by user")
            self._print_summary()
        except Exception as e:
            logger.error(f"❌ Error in drift monitor: {e}", exc_info=True)
        finally:
            consumer.close()
    
    def _print_summary(self):
        """Print summary of detected drifts."""
        if not self.drift_alerts:
            logger.info("📊 No drift detected during monitoring period")
            return
        
        logger.info("\n" + "="*60)
        logger.info("📊 DRIFT MONITORING SUMMARY")
        logger.info("="*60)
        
        for symbol, alerts in self.drift_alerts.items():
            logger.info(f"\n{symbol}: {len(alerts)} drift events")
            for alert in alerts[-5:]:  # Show last 5 alerts
                logger.info(f"  📍 {alert['timestamp']}: {', '.join(alert['alerts'])}")


if __name__ == "__main__":
    monitor = DriftMonitor(window_size=100, drift_threshold=2.0)
    monitor.run()
