"""
Databricks Integration Utilities for Stock Market MLOps Pipeline

This module provides helper functions to integrate Databricks with your
existing Kafka-based streaming pipeline.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

try:
    from databricks.sql import sql
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import GetWarehouseResponse
    HAS_DATABRICKS = True
except ImportError:
    HAS_DATABRICKS = False

logger = logging.getLogger(__name__)


class DatabricksConnector:
    """Handles Databricks connections and Delta Lake operations"""
    
    def __init__(self, 
                 host: Optional[str] = None,
                 token: Optional[str] = None,
                 warehouse_id: Optional[str] = None,
                 http_path: Optional[str] = None):
        """
        Initialize Databricks connector
        
        Args:
            host: Databricks workspace URL (defaults to DATABRICKS_HOST env var)
            token: Personal access token (defaults to DATABRICKS_TOKEN env var)
            warehouse_id: SQL Warehouse ID (defaults to DATABRICKS_WAREHOUSE_ID)
            http_path: HTTP path to warehouse (defaults to DATABRICKS_HTTP_PATH)
        """
        if not HAS_DATABRICKS:
            logger.warning("Databricks SDK not installed. Install with: pip install databricks-sdk")
            return
        
        self.host = host or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")
        self.warehouse_id = warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID")
        self.http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH")
        
        if not all([self.host, self.token]):
            raise ValueError("Databricks host and token are required")
        
        self.connection = None
        self.ws_client = None
        self._connect()
    
    def _connect(self):
        """Establish Databricks connections"""
        try:
            # SQL connection
            if self.warehouse_id and self.http_path:
                self.connection = sql.connect(
                    server_hostname=self.host.replace("https://", "").replace("/", ""),
                    http_path=self.http_path,
                    auth_type="pat",
                    personal_access_token=self.token
                )
                logger.info("Connected to Databricks SQL Warehouse")
            
            # Workspace client
            self.ws_client = WorkspaceClient(
                host=self.host,
                token=self.token
            )
            logger.info("Connected to Databricks Workspace")
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            raise
    
    def query(self, sql_query: str) -> list[Dict[str, Any]]:
        """Execute SQL query and return results"""
        if not self.connection:
            raise ValueError("Databricks connection not established")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return results
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
        finally:
            cursor.close()
    
    def write_event_to_delta(self, 
                           catalog: str,
                           schema: str,
                           table: str,
                           event_data: Dict[str, Any]) -> bool:
        """
        Write a single event to Delta Lake table
        
        Args:
            catalog: Databricks catalog name
            schema: Schema name
            table: Table name
            event_data: Dictionary of event data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare values
            columns = ", ".join(event_data.keys())
            values = ", ".join([
                f"'{v}'" if isinstance(v, str) else str(v)
                for v in event_data.values()
            ])
            
            # Insert event
            insert_sql = f"""
                INSERT INTO {catalog}.{schema}.{table}
                ({columns})
                VALUES ({values})
            """
            
            self.query(insert_sql)
            logger.debug(f"Event written to {catalog}.{schema}.{table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write event: {e}")
            return False
    
    def read_table(self, 
                   catalog: str,
                   schema: str,
                   table: str,
                   limit: int = 100) -> list[Dict[str, Any]]:
        """Read data from Delta table"""
        try:
            sql_query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
            return self.query(sql_query)
        except Exception as e:
            logger.error(f"Failed to read table: {e}")
            raise
    
    def create_table_if_not_exists(self,
                                   catalog: str,
                                   schema: str,
                                   table: str,
                                   columns: Dict[str, str]) -> bool:
        """
        Create Delta table if it doesn't exist
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            columns: Dict of column_name: data_type pairs
                    e.g., {"timestamp": "TIMESTAMP", "price": "DOUBLE"}
        """
        try:
            # Build column definitions
            col_defs = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
                    {col_defs}
                ) USING DELTA
            """
            
            self.query(create_sql)
            logger.info(f"Table {catalog}.{schema}.{table} created or already exists")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    def close(self):
        """Close Databricks connections"""
        if self.connection:
            self.connection.close()
            logger.info("Databricks connection closed")


class KafkaEventsToDelta:
    """Bridge Kafka events to Databricks Delta Lake"""
    
    def __init__(self, databricks_connector: DatabricksConnector):
        self.db = databricks_connector
    
    def write_market_event(self, event: Dict[str, Any]) -> bool:
        """
        Write market data event to Delta
        
        Args:
            event: Market event dict with keys:
                   {timestamp, symbol, open, high, low, close, volume}
        """
        return self.db.write_event_to_delta(
            catalog="stock_market_mlops",
            schema="raw_data",
            table="kafka_events",
            event_data={
                "timestamp": event.get("timestamp"),
                "symbol": event.get("symbol"),
                "open": event.get("open"),
                "high": event.get("high"),
                "low": event.get("low"),
                "close": event.get("close"),
                "volume": event.get("volume"),
                "ingestion_time": datetime.utcnow().isoformat()
            }
        )
    
    def write_prediction_event(self, prediction: Dict[str, Any]) -> bool:
        """Write prediction event to Delta"""
        return self.db.write_event_to_delta(
            catalog="stock_market_mlops",
            schema="predictions",
            table="inference_logs",
            event_data={
                "timestamp": datetime.utcnow().isoformat(),
                "symbol": prediction.get("symbol"),
                "prediction": prediction.get("value"),
                "confidence": prediction.get("confidence"),
                "model_version": prediction.get("model_version")
            }
        )
    
    def write_drift_alert(self, alert: Dict[str, Any]) -> bool:
        """Write drift detection alert to Delta"""
        return self.db.write_event_to_delta(
            catalog="stock_market_mlops",
            schema="predictions",
            table="drift_alerts",
            event_data={
                "timestamp": alert.get("timestamp", datetime.utcnow().isoformat()),
                "metric": alert.get("metric"),
                "z_score": alert.get("z_score"),
                "value": alert.get("value"),
                "threshold": alert.get("threshold"),
                "symbol": alert.get("symbol")
            }
        )


def setup_databricks_tables(db: DatabricksConnector) -> bool:
    """Initialize required Databricks tables for the pipeline"""
    
    try:
        # Raw Kafka events table
        db.create_table_if_not_exists(
            catalog="stock_market_mlops",
            schema="raw_data",
            table="kafka_events",
            columns={
                "timestamp": "TIMESTAMP",
                "symbol": "STRING",
                "open": "DOUBLE",
                "high": "DOUBLE",
                "low": "DOUBLE",
                "close": "DOUBLE",
                "volume": "LONG",
                "ingestion_time": "STRING"
            }
        )
        
        # Inference logs table
        db.create_table_if_not_exists(
            catalog="stock_market_mlops",
            schema="predictions",
            table="inference_logs",
            columns={
                "timestamp": "STRING",
                "symbol": "STRING",
                "prediction": "DOUBLE",
                "confidence": "DOUBLE",
                "model_version": "STRING"
            }
        )
        
        # Drift alerts table
        db.create_table_if_not_exists(
            catalog="stock_market_mlops",
            schema="predictions",
            table="drift_alerts",
            columns={
                "timestamp": "STRING",
                "metric": "STRING",
                "z_score": "DOUBLE",
                "value": "DOUBLE",
                "threshold": "DOUBLE",
                "symbol": "STRING"
            }
        )
        
        logger.info("✅ All Databricks tables initialized")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize tables: {e}")
        return False


if __name__ == "__main__":
    # Example usage
    import logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize connector
        db = DatabricksConnector()
        
        # Setup tables
        setup_databricks_tables(db)
        
        # Test write
        bridge = KafkaEventsToDelta(db)
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": "AAPL",
            "open": 150.0,
            "high": 152.5,
            "low": 149.8,
            "close": 151.2,
            "volume": 1000000
        }
        
        success = bridge.write_market_event(event)
        print(f"Event write: {'✅ Success' if success else '❌ Failed'}")
        
        # Test read
        results = db.read_table("stock_market_mlops", "raw_data", "kafka_events", limit=5)
        print(f"Read {len(results)} events from Delta")
        
        db.close()
        
    except Exception as e:
        print(f"Error: {e}")
