"""Unit tests for service-layer business rules."""

from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import insert, select

from app.crud import TradeService
from app.models import Trade
from app.schemas import TradeUpdate, TradeCreate


class TestCoreTradeRules:
    """Test core business rules requested for the trade store"""

    def test_lower_version_trade_is_rejected(self, test_db):
        original = TradeCreate(
            trade_id="T-V1",
            version=2,
            counter_party_id="CP-1",
            book_id="B1",
            maturity_date=datetime.now() + timedelta(days=30),
            created_date=datetime.now(),
            expired=False,
        )
        TradeService.create_trade(test_db, original)

        lower_version = TradeCreate(
            trade_id="T-V1",
            version=1,
            counter_party_id="CP-1",
            book_id="B1",
            maturity_date=datetime.now() + timedelta(days=30),
            created_date=datetime.now(),
            expired=False,
        )

        with pytest.raises(HTTPException) as exc_info:
            TradeService.create_trade(test_db, lower_version)

        assert exc_info.value.status_code == 400
        assert "lower" in str(exc_info.value.detail).lower()

    def test_same_version_trade_replaces_existing_record(self, test_db):
        original = TradeCreate(
            trade_id="T-V2",
            version=1,
            counter_party_id="CP-OLD",
            book_id="B1",
            maturity_date=datetime.now() + timedelta(days=30),
            created_date=datetime.now(),
            expired=False,
        )
        TradeService.create_trade(test_db, original)

        replacement = TradeCreate(
            trade_id="T-V2",
            version=1,
            counter_party_id="CP-NEW",
            book_id="B2",
            maturity_date=datetime.now() + timedelta(days=60),
            created_date=datetime.now(),
            expired=False,
        )
        TradeService.create_trade(test_db, replacement)

        saved = TradeService.get_trade(test_db, "T-V2", 1)
        assert saved is not None
        assert saved.counter_party_id == "CP-NEW"
        assert saved.book_id == "B2"

        total_rows = test_db.execute(select(Trade).where(Trade.trade_id == "T-V2")).scalars().all()
        assert len(total_rows) == 1

    def test_trade_with_maturity_date_before_today_is_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            TradeCreate(
                trade_id="T-PAST",
                version=1,
                counter_party_id="CP-1",
                book_id="B1",
                maturity_date=datetime.now() - timedelta(days=1),
                created_date=datetime.now(),
                expired=False,
            )

        assert "cannot be before today's date" in str(exc_info.value)

    def test_surpassed_maturity_date_trades_are_marked_expired(self, test_db):
        test_db.execute(
            insert(Trade).values(
                trade_id="EXP-AUTO-1",
                version=1,
                counter_party_id="CP-1",
                book_id="B1",
                maturity_date=datetime.now() - timedelta(days=1),
                created_date=datetime.now() - timedelta(days=2),
                expired=False,
            )
        )
        test_db.execute(
            insert(Trade).values(
                trade_id="EXP-AUTO-2",
                version=1,
                counter_party_id="CP-1",
                book_id="B1",
                maturity_date=datetime.now() + timedelta(days=1),
                created_date=datetime.now(),
                expired=False,
            )
        )
        test_db.commit()

        updated_count = TradeService.mark_expired_trades(test_db)

        assert updated_count == 1
        expired_trade = TradeService.get_trade(test_db, "EXP-AUTO-1", 1)
        active_trade = TradeService.get_trade(test_db, "EXP-AUTO-2", 1)

        assert expired_trade is not None and expired_trade.expired is True
        assert active_trade is not None and active_trade.expired is False


class TestTradeBusinessRules:
    """Test trade update rules in service layer"""

    def test_expired_trade_update_fails(self, test_db):
        test_db.execute(
            insert(Trade).values(
                trade_id="EXP-1",
                version=1,
                counter_party_id="CP-1",
                book_id="B1",
                maturity_date=datetime.now() + timedelta(days=30),
                created_date=datetime.now(),
                expired=True,
            )
        )
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
        test_db.execute(
            insert(Trade).values(
                trade_id="EXP-2",
                version=1,
                counter_party_id="CP-1",
                book_id="B1",
                maturity_date=datetime.now() + timedelta(days=30),
                created_date=datetime.now(),
                expired=True,
            )
        )
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
