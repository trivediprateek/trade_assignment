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
            
            assert config['bootstrap.servers'] == 'localhost:9093'
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


class TestProcessTradeMessage:
    """Test processing individual trade messages"""
    
    def test_process_valid_trade_message(self, test_db):
        """Test processing a valid CREATE trade message"""
        message_data = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T1001",
                "version": 1,
                "counter_party_id": "CP-1001",
                "book_id": "BOOK-1",
                "maturity_date": str(date.today() + timedelta(days=30)),
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        message_value = json.dumps(message_data)
        
        # Process the message
        result = process_trade_message(message_value, test_db)
        
        # Assertions
        assert result is True
        
        # Verify trade was created in database
        db_trade = test_db.query(Trade).filter(
            Trade.trade_id == "T1001",
            Trade.version == 1
        ).first()
        
        assert db_trade is not None
        assert db_trade.counter_party_id == "CP-1001"
        assert db_trade.book_id == "BOOK-1"
    
    def test_process_invalid_json(self, test_db):
        """Test processing invalid JSON message"""
        invalid_json = "{invalid json"
        
        result = process_trade_message(invalid_json, test_db)
        
        assert result is False
    
    def test_process_message_with_missing_fields(self, test_db):
        """Test processing message with missing required fields"""
        incomplete_trade = {
            "trade_id": "T1002",
            "version": 1
            # Missing other required fields
        }
        
        message_value = json.dumps(incomplete_trade)
        
        result = process_trade_message(message_value, test_db)
        
        assert result is False
    
    def test_process_message_with_past_maturity_date(self, test_db):
        """Test processing message with past maturity date (validation error)"""
        message_data = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T1003",
                "version": 1,
                "counter_party_id": "CP-1003",
                "book_id": "BOOK-1",
                "maturity_date": "2020-01-01",  # Past date
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        message_value = json.dumps(message_data)
        
        result = process_trade_message(message_value, test_db)
        
        assert result is False
        
        # Verify no trade was created
        db_trade = test_db.query(Trade).filter(
            Trade.trade_id == "T1003"
        ).first()
        
        assert db_trade is None
    
    def test_process_message_with_version_conflict(self, test_db):
        """Test processing message with lower version than existing"""
        # Create initial trade version 2
        message_data_v2 = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T1004",
                "version": 2,
                "counter_party_id": "CP-1004",
                "book_id": "BOOK-1",
                "maturity_date": str(date.today() + timedelta(days=30)),
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        message_v2 = json.dumps(message_data_v2)
        result = process_trade_message(message_v2, test_db)
        assert result is True
        
        # Try to create version 1 (should fail)
        message_data_v1 = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T1004",
                "version": 1,  # Lower version
                "counter_party_id": "CP-1004",
                "book_id": "BOOK-1",
                "maturity_date": str(date.today() + timedelta(days=30)),
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        message_v1 = json.dumps(message_data_v1)
        result = process_trade_message(message_v1, test_db)
        
        assert result is False
        
        # Verify only version 2 exists
        trades = test_db.query(Trade).filter(
            Trade.trade_id == "T1004"
        ).all()
        
        assert len(trades) == 1
        assert trades[0].version == 2
    
    def test_process_multiple_versions_sequentially(self, test_db):
        """Test processing multiple versions of same trade in order"""
        # Process versions 1, 2, 3
        for version in [1, 2, 3]:
            message_data = {
                "operation": "CREATE",
                "data": {
                    "trade_id": "T1005",
                    "version": version,
                    "counter_party_id": "CP-1005",
                    "book_id": "BOOK-1",
                    "maturity_date": str(date.today() + timedelta(days=30)),
                    "created_date": str(date.today()),
                    "expired": False
                }
            }
            message = json.dumps(message_data)
            result = process_trade_message(message, test_db)
            assert result is True
        
        # Verify all versions exist
        trades = test_db.query(Trade).filter(
            Trade.trade_id == "T1005"
        ).order_by(Trade.version).all()
        
        assert len(trades) == 3
        assert [t.version for t in trades] == [1, 2, 3]
    
    def test_process_duplicate_message(self, test_db):
        """Test processing duplicate message (same trade_id and version)"""
        message_data = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T1006",
                "version": 1,
                "counter_party_id": "CP-1006",
                "book_id": "BOOK-1",
                "maturity_date": str(date.today() + timedelta(days=30)),
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        message = json.dumps(message_data)
        
        # First message should succeed
        result1 = process_trade_message(message, test_db)
        assert result1 is True
        
        # Duplicate should fail (composite primary key violation)
        result2 = process_trade_message(message, test_db)
        assert result2 is False


class TestKafkaConsumerIntegration:
    """Integration tests for Kafka consumer (with mocked Kafka)"""
    
    @patch('app.kafka_consumer.Consumer')
    def test_consumer_initialization(self, mock_consumer_class):
        """Test consumer initialization with correct configuration"""
        from app.kafka_consumer import get_kafka_config
        
        config = get_kafka_config()
        
        assert config['bootstrap.servers'] == 'localhost:9093'
        assert config['group.id'] == 'trade-consumer-group'
    
    def test_message_processing_workflow(self, test_db):
        """Test complete message processing workflow"""
        # Simulate receiving messages from Kafka
        messages = [
            {
                "operation": "CREATE",
                "data": {
                    "trade_id": "T2001",
                    "version": 1,
                    "counter_party_id": "CP-2001",
                    "book_id": "BOOK-1",
                    "maturity_date": str(date.today() + timedelta(days=30)),
                    "created_date": str(date.today()),
                    "expired": False
                }
            },
            {
                "operation": "CREATE",
                "data": {
                    "trade_id": "T2002",
                    "version": 1,
                    "counter_party_id": "CP-2002",
                    "book_id": "BOOK-2",
                    "maturity_date": str(date.today() + timedelta(days=60)),
                    "created_date": str(date.today()),
                    "expired": False
                }
            }
        ]
        
        success_count = 0
        for msg in messages:
            message_value = json.dumps(msg)
            if process_trade_message(message_value, test_db):
                success_count += 1
        
        assert success_count == 2
        
        # Verify both trades in database
        trades = test_db.query(Trade).filter(
            Trade.trade_id.in_(["T2001", "T2002"])
        ).all()
        
        assert len(trades) == 2


class TestUpdateOperations:
    """Test UPDATE operation processing"""
    
    def test_process_update_operation(self, test_db):
        """Test processing an UPDATE operation"""
        # First create a trade
        create_message = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T5001",
                "version": 1,
                "counter_party_id": "CP-5001",
                "book_id": "BOOK-1",
                "maturity_date": str(date.today() + timedelta(days=30)),
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        result = process_trade_message(json.dumps(create_message), test_db)
        assert result is True
        
        # Now update the trade
        update_message = {
            "operation": "UPDATE",
            "data": {
                "trade_id": "T5001",
                "version": 1,
                "counter_party_id": "CP-UPDATED"
            }
        }
        
        result = process_trade_message(json.dumps(update_message), test_db)
        assert result is True
        
        # Verify the trade was updated
        db_trade = test_db.query(Trade).filter(
            Trade.trade_id == "T5001",
            Trade.version == 1
        ).first()
        
        assert db_trade is not None
        assert db_trade.counter_party_id == "CP-UPDATED"
    
    def test_update_nonexistent_trade_fails(self, test_db):
        """Test updating a trade that doesn't exist fails"""
        update_message = {
            "operation": "UPDATE",
            "data": {
                "trade_id": "NONEXISTENT",
                "version": 1,
                "counter_party_id": "CP-UPDATED"
            }
        }
        
        result = process_trade_message(json.dumps(update_message), test_db)
        assert result is False


class TestDeleteOperations:
    """Test DELETE operation processing"""
    
    def test_process_delete_operation(self, test_db):
        """Test processing a DELETE operation"""
        # First create a trade
        create_message = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T6001",
                "version": 1,
                "counter_party_id": "CP-6001",
                "book_id": "BOOK-1",
                "maturity_date": str(date.today() + timedelta(days=30)),
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        result = process_trade_message(json.dumps(create_message), test_db)
        assert result is True
        
        # Verify trade exists
        db_trade = test_db.query(Trade).filter(
            Trade.trade_id == "T6001",
            Trade.version == 1
        ).first()
        assert db_trade is not None
        
        # Now delete the trade
        delete_message = {
            "operation": "DELETE",
            "data": {
                "trade_id": "T6001",
                "version": 1
            }
        }
        
        result = process_trade_message(json.dumps(delete_message), test_db)
        assert result is True
        
        # Verify the trade was deleted
        db_trade = test_db.query(Trade).filter(
            Trade.trade_id == "T6001",
            Trade.version == 1
        ).first()
        
        assert db_trade is None
    
    def test_delete_nonexistent_trade_fails(self, test_db):
        """Test deleting a trade that doesn't exist fails"""
        delete_message = {
            "operation": "DELETE",
            "data": {
                "trade_id": "NONEXISTENT",
                "version": 1
            }
        }
        
        result = process_trade_message(json.dumps(delete_message), test_db)
        assert result is False


class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_database_error_handling(self, test_db):
        """Test handling of database errors during processing"""
        message_data = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T3001",
                "version": 1,
                "counter_party_id": "CP-3001",
                "book_id": "BOOK-1",
                "maturity_date": str(date.today() + timedelta(days=30)),
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        message = json.dumps(message_data)
        
        # Mock database session to raise an exception
        mock_db = Mock()
        mock_db.query.side_effect = Exception("Database connection error")
        
        result = process_trade_message(message, mock_db)
        
        assert result is False
    
    def test_malformed_date_format(self, test_db):
        """Test handling of malformed date formats"""
        message_data = {
            "operation": "CREATE",
            "data": {
                "trade_id": "T3002",
                "version": 1,
                "counter_party_id": "CP-3002",
                "book_id": "BOOK-1",
                "maturity_date": "not-a-date",  # Invalid format
                "created_date": str(date.today()),
                "expired": False
            }
        }
        
        message = json.dumps(message_data)
        
        result = process_trade_message(message, test_db)
        
        assert result is False
