"""
Unit tests for Kafka consumer functionality

Tests the Kafka consumer's ability to process trade messages
using mocked Kafka messages and database sessions.
"""

import json
from datetime import date, timedelta
from unittest.mock import Mock, patch, MagicMock

import pytest

from app.kafka_consumer import process_trade_message, get_kafka_config
from app.models import Trade
from app.schemas import TradeCreate


class TestKafkaConfig:
    """Test Kafka configuration"""
    
    def test_get_kafka_config_defaults(self):
        """Test default Kafka configuration values"""
        with patch.dict('os.environ', {}, clear=True):
            config = get_kafka_config()
            
            assert config['bootstrap.servers'] == 'localhost:9092'
            assert config['group.id'] == 'trade-consumer-group'
            assert config['auto.offset.reset'] == 'earliest'
            assert config['enable.auto.commit'] is False
    
    def test_get_kafka_config_from_env(self):
        """Test Kafka configuration from environment variables"""
        env_vars = {
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
            'KAFKA_GROUP_ID': 'test-group',
        }
        
        with patch.dict('os.environ', env_vars):
            config = get_kafka_config()
            
            assert config['bootstrap.servers'] == 'kafka:9092'
            assert config['group.id'] == 'test-group'

