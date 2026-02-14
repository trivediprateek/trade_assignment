"""
Unit tests for Kafka consumer functionality

Tests the Kafka consumer's ability to process trade messages
using mocked Kafka messages and database sessions.
"""

import json
from datetime import date, timedelta
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from app.kafka_consumer import process_trade_message, get_kafka_config
from app.models import Trade
from app.schemas import TradeUpdate
from app.crud import TradeService


class TestKafkaConfig:
    """Test Kafka configuration"""

    def test_get_kafka_config_defaults(self):
        """Test default Kafka configuration values"""
        with patch.dict("os.environ", {}, clear=True):
            config = get_kafka_config()

            assert config["bootstrap.servers"] == "localhost:9092"
            assert config["group.id"] == "trade-consumer-group"
            assert config["auto.offset.reset"] == "earliest"
            assert config["enable.auto.commit"] is False

    def test_get_kafka_config_from_env(self):
        """Test Kafka configuration from environment variables"""
        env_vars = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_GROUP_ID": "test-group",
        }

        with patch.dict("os.environ", env_vars):
            config = get_kafka_config()

            assert config["bootstrap.servers"] == "kafka:9092"
            assert config["group.id"] == "test-group"


class TestExpiredTradeUpdateRules:
    """Test expired trade update rules in service layer"""

    def test_expired_trade_update_fails(self, test_db):
        trade = Trade(
            trade_id="EXP-1",
            version=1,
            counter_party_id="CP-1",
            book_id="B1",
            maturity_date=date.today() + timedelta(days=30),
            created_date=date.today(),
            expired=True,
        )
        test_db.add(trade)
        test_db.commit()

        with pytest.raises(HTTPException) as exc_info:
            TradeService.update_trade(
                test_db,
                "EXP-1",
                1,
                TradeUpdate(book_id="B2"),
            )

        assert exc_info.value.status_code == 400
        assert "expired" in str(exc_info.value.detail).lower()

    def test_expired_trade_update_book_id_still_fails(self, test_db):
        trade = Trade(
            trade_id="EXP-2",
            version=1,
            counter_party_id="CP-1",
            book_id="B1",
            maturity_date=date.today() + timedelta(days=30),
            created_date=date.today(),
            expired=True,
        )
        test_db.add(trade)
        test_db.commit()

        with pytest.raises(HTTPException) as exc_info:
            TradeService.update_trade(
                test_db,
                "EXP-2",
                1,
                TradeUpdate(book_id="B2"),
            )

        assert exc_info.value.status_code == 400
        assert "expired" in str(exc_info.value.detail).lower()


class TestKafkaMessagePersistence:
    """Test Kafka message processing persists expected DB changes"""

    def test_process_create_message_inserts_trade_in_db(self, test_db):
        payload = {
            "operation": "CREATE",
            "data": {
                "trade_id": "KAFKA-CREATE-1",
                "version": 1,
                "counter_party_id": "CP-NEW",
                "book_id": "BOOK-1",
                "maturity_date": (date.today() + timedelta(days=30)).isoformat(),
                "created_date": date.today().isoformat(),
                "expired": False,
            },
        }

        success = process_trade_message(json.dumps(payload), test_db)

        assert success is True
        saved = (
            test_db.query(Trade)
            .filter(
                Trade.trade_id == "KAFKA-CREATE-1",
                Trade.version == 1,
            )
            .first()
        )
        assert saved is not None
        assert saved.counter_party_id == "CP-NEW"
        assert saved.book_id == "BOOK-1"

    def test_process_update_message_updates_trade_in_db(self, test_db):
        existing = Trade(
            trade_id="KAFKA-UPD-1",
            version=1,
            counter_party_id="CP-OLD",
            book_id="BOOK-OLD",
            maturity_date=date.today() + timedelta(days=60),
            created_date=date.today(),
            expired=False,
        )
        test_db.add(existing)
        test_db.commit()

        payload = {
            "operation": "UPDATE",
            "data": {
                "trade_id": "KAFKA-UPD-1",
                "version": 1,
                "counter_party_id": "CP-NEW",
            },
        }

        success = process_trade_message(json.dumps(payload), test_db)

        assert success is True
        updated = (
            test_db.query(Trade)
            .filter(
                Trade.trade_id == "KAFKA-UPD-1",
                Trade.version == 1,
            )
            .first()
        )
        assert updated is not None
        assert updated.counter_party_id == "CP-NEW"
        assert updated.book_id == "BOOK-OLD"

    def test_process_delete_message_deletes_trade_from_db(self, test_db):
        existing = Trade(
            trade_id="KAFKA-DEL-1",
            version=1,
            counter_party_id="CP-1",
            book_id="BOOK-1",
            maturity_date=date.today() + timedelta(days=60),
            created_date=date.today(),
            expired=False,
        )
        test_db.add(existing)
        test_db.commit()

        payload = {
            "operation": "DELETE",
            "data": {
                "trade_id": "KAFKA-DEL-1",
                "version": 1,
            },
        }

        success = process_trade_message(json.dumps(payload), test_db)

        assert success is True
        deleted = (
            test_db.query(Trade)
            .filter(
                Trade.trade_id == "KAFKA-DEL-1",
                Trade.version == 1,
            )
            .first()
        )
        assert deleted is None
