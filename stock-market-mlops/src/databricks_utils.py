"""
Databricks Integration Utilities for Stock Market MLOps Pipeline

This module provides helper functions to integrate Databricks with your
existing Kafka-based streaming pipeline.
"""

import os
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Sequence

from src.config import DATABRICKS_CATALOG, DATABRICKS_PREDICTIONS_SCHEMA, DATABRICKS_RAW_SCHEMA

try:
    from databricks.sql import sql
    from databricks.sdk import WorkspaceClient
    HAS_DATABRICKS = True
except ImportError:
    HAS_DATABRICKS = False

logger = logging.getLogger(__name__)
_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 1.0


def _validate_identifier(identifier: str, label: str) -> str:
    if not _SQL_IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"Invalid {label}: {identifier!r}")
    return identifier


def _quote_identifier(identifier: str, label: str) -> str:
    return f"`{_validate_identifier(identifier, label)}`"


def _qualified_name(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        (
            _quote_identifier(catalog, "catalog"),
            _quote_identifier(schema, "schema"),
            _quote_identifier(table, "table"),
        )
    )


def _normalize_server_hostname(host: str) -> str:
    return host.replace("https://", "").replace("http://", "").rstrip("/")


def _to_datetime(value: Any) -> Any:
    if isinstance(value, datetime) or value is None:
        return value
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    return value


class DatabricksConnector:
    """Handles Databricks connections and Delta Lake operations"""
    
    def __init__(self, 
                 host: Optional[str] = None,
                 token: Optional[str] = None,
                 warehouse_id: Optional[str] = None,
                 http_path: Optional[str] = None,
                 max_retries: int = DEFAULT_MAX_RETRIES,
                 retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS):
        """
        Initialize Databricks connector
        
        Args:
            host: Databricks workspace URL (defaults to DATABRICKS_HOST env var)
            token: Personal access token (defaults to DATABRICKS_TOKEN env var)
            warehouse_id: SQL Warehouse ID (defaults to DATABRICKS_WAREHOUSE_ID)
            http_path: HTTP path to warehouse (defaults to DATABRICKS_HTTP_PATH)
            max_retries: Number of attempts for transient Databricks failures
            retry_delay_seconds: Base delay between retries
        """
        self.connection = None
        self.ws_client = None
        self.host = host or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")
        self.warehouse_id = warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID")
        self.http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH")
        self.max_retries = max(1, max_retries)
        self.retry_delay_seconds = max(0.0, retry_delay_seconds)

        if not HAS_DATABRICKS:
            logger.warning("Databricks SDK not installed. Install with: pip install databricks-sdk")
            return

        if not all([self.host, self.token]):
            raise ValueError("Databricks host and token are required")

        self._connect()
    
    def _connect(self):
        """Establish Databricks connections"""
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                # SQL connection
                if self.warehouse_id and self.http_path:
                    self.connection = sql.connect(
                        server_hostname=_normalize_server_hostname(self.host),
                        http_path=self.http_path,
                        access_token=self.token,
                    )
                    logger.info("Connected to Databricks SQL Warehouse")

                # Workspace client
                self.ws_client = WorkspaceClient(
                    host=self.host,
                    token=self.token
                )
                logger.info("Connected to Databricks Workspace")
                return
            except Exception as e:
                last_error = e
                logger.warning(
                    "Databricks connection attempt %s/%s failed: %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                self._disconnect()
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay_seconds * attempt)

        logger.error(f"Failed to connect to Databricks: {last_error}")
        raise last_error

    def _disconnect(self) -> None:
        if self.connection:
            self.connection.close()
        self.connection = None
        self.ws_client = None

    def _retry_delay(self, attempt: int) -> None:
        if attempt < self.max_retries:
            time.sleep(self.retry_delay_seconds * attempt)

    def _reconnect(self) -> None:
        self._disconnect()
        self._connect()

    def _run_sql(
        self,
        sql_query: str,
        parameters: Optional[Sequence[Any] | Mapping[str, Any]] = None,
        *,
        fetch_results: bool = False,
        many: bool = False,
    ) -> list[Dict[str, Any]]:
        """Execute SQL with retry handling."""
        if not self.connection:
            raise ValueError("Databricks connection not established")

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with self.connection.cursor() as cursor:
                    if many:
                        if parameters is None:
                            raise ValueError("Batch execution requires parameters")
                        cursor.executemany(sql_query, parameters)
                    else:
                        if parameters is None:
                            cursor.execute(sql_query)
                        else:
                            cursor.execute(sql_query, parameters=parameters)

                    if not fetch_results:
                        return []

                    if cursor.description is None:
                        return []

                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
            except Exception as e:
                last_error = e
                logger.warning(
                    "Databricks SQL attempt %s/%s failed: %s",
                    attempt,
                    self.max_retries,
                    e,
                )
                if attempt < self.max_retries:
                    self._reconnect()
                    self._retry_delay(attempt)

        logger.error(f"Databricks SQL failed: {last_error}")
        raise last_error

    def query(
        self,
        sql_query: str,
        parameters: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> list[Dict[str, Any]]:
        """Execute SQL query and return results."""
        return self._run_sql(sql_query, parameters=parameters, fetch_results=True)

    def execute(
        self,
        sql_statement: str,
        parameters: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> None:
        """Execute a SQL statement that does not return rows."""
        self._run_sql(sql_statement, parameters=parameters, fetch_results=False)

    def execute_many(
        self,
        sql_statement: str,
        parameters: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> None:
        """Execute a SQL statement for many parameter sets."""
        self._run_sql(sql_statement, parameters=parameters, many=True)
    
    def write_event_to_delta(self, 
                           catalog: str,
                           schema: str,
                           table: str,
                           event_data: Mapping[str, Any]) -> bool:
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
            columns = list(event_data.keys())
            insert_sql = (
                f"INSERT INTO {_qualified_name(catalog, schema, table)} "
                f"({', '.join(_quote_identifier(column, 'column') for column in columns)}) "
                f"VALUES ({', '.join(['?'] * len(columns))})"
            )
            self.execute(insert_sql, parameters=[event_data[column] for column in columns])
            logger.debug("Event written to %s.%s.%s", catalog, schema, table)
            return True
            
        except Exception as e:
            logger.error(f"Failed to write event: {e}")
            return False

    def write_events_to_delta(
        self,
        catalog: str,
        schema: str,
        table: str,
        events: Sequence[Mapping[str, Any]],
    ) -> bool:
        """Write multiple events to Delta Lake table."""
        try:
            if not events:
                return True

            columns = list(events[0].keys())
            insert_sql = (
                f"INSERT INTO {_qualified_name(catalog, schema, table)} "
                f"({', '.join(_quote_identifier(column, 'column') for column in columns)}) "
                f"VALUES ({', '.join(['?'] * len(columns))})"
            )
            parameters = [
                [event.get(column) for column in columns]
                for event in events
            ]
            self.execute_many(insert_sql, parameters)
            logger.debug("Wrote %s events to %s.%s.%s", len(events), catalog, schema, table)
            return True
        except Exception as e:
            logger.error(f"Failed to write events: {e}")
            return False
    
    def read_table(self, 
                   catalog: str,
                   schema: str,
                   table: str,
                   limit: int = 100) -> list[Dict[str, Any]]:
        """Read data from Delta table"""
        try:
            sql_query = f"SELECT * FROM {_qualified_name(catalog, schema, table)} LIMIT ?"
            return self.query(sql_query, parameters=[limit])
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
            col_defs = ", ".join(
                [f"{_quote_identifier(name, 'column')} {dtype}" for name, dtype in columns.items()]
            )
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {_qualified_name(catalog, schema, table)} (
                    {col_defs}
                ) USING DELTA
            """
            
            self.execute(create_sql)
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

    def _market_event_payload(self, event: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "timestamp": _to_datetime(event.get("timestamp")),
            "symbol": event.get("symbol"),
            "open": event.get("open"),
            "high": event.get("high"),
            "low": event.get("low"),
            "close": event.get("close"),
            "volume": event.get("volume"),
            "ingestion_time": datetime.utcnow().isoformat(),
        }

    def _prediction_payload(self, prediction: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": prediction.get("symbol"),
            "prediction": prediction.get("value"),
            "confidence": prediction.get("confidence"),
            "model_version": prediction.get("model_version"),
        }

    def _drift_alert_payload(self, alert: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "timestamp": alert.get("timestamp", datetime.utcnow().isoformat()),
            "metric": alert.get("metric"),
            "z_score": alert.get("z_score"),
            "value": alert.get("value"),
            "threshold": alert.get("threshold"),
            "symbol": alert.get("symbol"),
        }
    
    def write_market_event(self, event: Mapping[str, Any]) -> bool:
        """
        Write market data event to Delta
        
        Args:
            event: Market event dict with keys:
                   {timestamp, symbol, open, high, low, close, volume}
        """
        return self.write_market_events([event])

    def write_market_events(self, events: Sequence[Mapping[str, Any]]) -> bool:
        """Write multiple market events to Delta."""
        return self.db.write_events_to_delta(
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_RAW_SCHEMA,
            table="kafka_events",
            events=[self._market_event_payload(event) for event in events],
        )
    
    def write_prediction_event(self, prediction: Mapping[str, Any]) -> bool:
        """Write prediction event to Delta"""
        return self.db.write_event_to_delta(
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_PREDICTIONS_SCHEMA,
            table="inference_logs",
            event_data=self._prediction_payload(prediction),
        )
    
    def write_prediction_events(self, predictions: Sequence[Mapping[str, Any]]) -> bool:
        """Write multiple prediction events to Delta."""
        return self.db.write_events_to_delta(
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_PREDICTIONS_SCHEMA,
            table="inference_logs",
            events=[self._prediction_payload(prediction) for prediction in predictions],
        )

    def write_drift_alert(self, alert: Mapping[str, Any]) -> bool:
        """Write drift detection alert to Delta"""
        return self.db.write_event_to_delta(
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_PREDICTIONS_SCHEMA,
            table="drift_alerts",
            event_data=self._drift_alert_payload(alert),
        )

    def write_drift_alerts(self, alerts: Sequence[Mapping[str, Any]]) -> bool:
        """Write multiple drift alerts to Delta."""
        return self.db.write_events_to_delta(
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_PREDICTIONS_SCHEMA,
            table="drift_alerts",
            events=[self._drift_alert_payload(alert) for alert in alerts],
        )


def setup_databricks_tables(db: DatabricksConnector) -> bool:
    """Initialize required Databricks tables for the pipeline"""
    
    try:
        # Raw Kafka events table
        db.create_table_if_not_exists(
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_RAW_SCHEMA,
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
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_PREDICTIONS_SCHEMA,
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
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_PREDICTIONS_SCHEMA,
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
        results = db.read_table(DATABRICKS_CATALOG, DATABRICKS_RAW_SCHEMA, "kafka_events", limit=5)
        print(f"Read {len(results)} events from Delta")
        
        db.close()
        
    except Exception as e:
        print(f"Error: {e}")
