"""Tests for Databricks integration utilities."""

from datetime import datetime

from src.config import DATABRICKS_CATALOG, DATABRICKS_PREDICTIONS_SCHEMA, DATABRICKS_RAW_SCHEMA
from src.databricks_utils import DatabricksConnector, KafkaEventsToDelta, setup_databricks_tables


class FakeCursor:
    def __init__(self, rows=None, fail_first_execute=False):
        self.rows = rows or []
        self.fail_first_execute = fail_first_execute
        self.execute_calls = []
        self.executemany_calls = []
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, parameters=None):
        self.execute_calls.append((statement, parameters))
        if self.fail_first_execute and len(self.execute_calls) == 1:
            raise RuntimeError("temporary outage")
        self.description = [("symbol",), ("close",)]
        return self

    def executemany(self, statement, parameters):
        self.executemany_calls.append((statement, list(parameters)))
        return self

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def build_connector(connection):
    connector = object.__new__(DatabricksConnector)
    connector.connection = connection
    connector.ws_client = None
    connector.host = "test.example.databricks.com"
    connector.token = "token"
    connector.warehouse_id = None
    connector.http_path = None
    connector.max_retries = 2
    connector.retry_delay_seconds = 0.0
    return connector


def test_query_uses_context_manager_and_parameters():
    cursor = FakeCursor(rows=[("AAPL", 151.2)])
    connector = build_connector(FakeConnection(cursor))

    result = connector.query("SELECT symbol, close FROM prices WHERE symbol = ?", parameters=["AAPL"])

    assert result == [{"symbol": "AAPL", "close": 151.2}]
    assert cursor.execute_calls == [("SELECT symbol, close FROM prices WHERE symbol = ?", ["AAPL"])]


def test_write_event_to_delta_uses_parameterized_sql():
    cursor = FakeCursor()
    connector = build_connector(FakeConnection(cursor))
    event = {
        "timestamp": datetime(2026, 8, 5, 17, 10, 0),
        "symbol": "O'HARE",
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 100,
    }

    ok = connector.write_event_to_delta("catalog", "raw_data", "kafka_events", event)

    assert ok is True
    statement, parameters = cursor.execute_calls[0]
    assert statement.startswith("INSERT INTO `catalog`.`raw_data`.`kafka_events`")
    assert "O'HARE" not in statement
    assert statement.count("?") == len(event)
    assert parameters[1] == "O'HARE"


def test_write_events_to_delta_uses_executemany():
    cursor = FakeCursor()
    connector = build_connector(FakeConnection(cursor))
    events = [
        {"symbol": "AAPL", "close": 151.2},
        {"symbol": "MSFT", "close": 421.7},
    ]

    ok = connector.write_events_to_delta("catalog", "raw_data", "kafka_events", events)

    assert ok is True
    statement, parameters = cursor.executemany_calls[0]
    assert statement == "INSERT INTO `catalog`.`raw_data`.`kafka_events` (`symbol`, `close`) VALUES (?, ?)"
    assert parameters == [["AAPL", 151.2], ["MSFT", 421.7]]


def test_query_retries_after_transient_failure():
    cursor = FakeCursor(rows=[("AAPL", 151.2)], fail_first_execute=True)
    connector = build_connector(FakeConnection(cursor))
    reconnect_calls = []
    connector._reconnect = lambda: reconnect_calls.append(True)

    result = connector.query("SELECT symbol, close FROM prices WHERE symbol = ?", parameters=["AAPL"])

    assert result == [{"symbol": "AAPL", "close": 151.2}]
    assert len(cursor.execute_calls) == 2
    assert reconnect_calls == [True]


def test_setup_databricks_tables_uses_configured_catalog():
    captured = []

    class DummyDb:
        def create_table_if_not_exists(self, **kwargs):
            captured.append(kwargs)
            return True

    ok = setup_databricks_tables(DummyDb())

    assert ok is True
    assert captured[0]["catalog"] == DATABRICKS_CATALOG
    assert captured[0]["schema"] == DATABRICKS_RAW_SCHEMA
    assert captured[1]["schema"] == DATABRICKS_PREDICTIONS_SCHEMA
