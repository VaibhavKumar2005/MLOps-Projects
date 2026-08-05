"""Tests for configuration module."""

import pytest
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_RAW_TOPIC,
    KAFKA_FEATURES_TOPIC,
    DATABRICKS_CATALOG,
    DATABRICKS_RAW_SCHEMA,
    DATABRICKS_PREDICTIONS_SCHEMA,
)


def test_kafka_config_exists():
    """Test that Kafka configuration is properly loaded."""
    assert KAFKA_BOOTSTRAP_SERVERS is not None
    assert KAFKA_CLIENT_ID is not None
    assert KAFKA_RAW_TOPIC is not None
    assert KAFKA_FEATURES_TOPIC is not None


def test_kafka_bootstrap_servers_format():
    """Test that bootstrap servers is a list."""
    assert isinstance(KAFKA_BOOTSTRAP_SERVERS, list)
    assert len(KAFKA_BOOTSTRAP_SERVERS) > 0


def test_kafka_topics_format():
    """Test that topic names are strings."""
    assert isinstance(KAFKA_RAW_TOPIC, str)
    assert isinstance(KAFKA_FEATURES_TOPIC, str)
    assert len(KAFKA_RAW_TOPIC) > 0
    assert len(KAFKA_FEATURES_TOPIC) > 0


def test_databricks_config_exists():
    """Test that Databricks configuration is properly loaded."""
    assert isinstance(DATABRICKS_CATALOG, str)
    assert isinstance(DATABRICKS_RAW_SCHEMA, str)
    assert isinstance(DATABRICKS_PREDICTIONS_SCHEMA, str)
    assert len(DATABRICKS_CATALOG) > 0
    assert len(DATABRICKS_RAW_SCHEMA) > 0
    assert len(DATABRICKS_PREDICTIONS_SCHEMA) > 0
