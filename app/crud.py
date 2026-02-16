from sqlalchemy.orm import Session
from sqlalchemy import select, insert, update, delete
from app.models import Trade
from app.schemas import TradeCreate, TradeUpdate
from datetime import datetime, date
from typing import List, Optional
from fastapi import HTTPException, status
from sqlalchemy.sql import func
import logging


logger = logging.getLogger(__name__)


class TradeService:
    """Service layer for trade operations"""

    @staticmethod
    def _ctx(trace_id: Optional[str]) -> str:
        return f"[trace_id={trace_id}] " if trace_id else ""

    @staticmethod
    def get_trade(db: Session, trade_id: str, version: int) -> Optional[Trade]:
        """Get a specific trade by ID and version"""
        stmt = select(Trade).where(Trade.trade_id == trade_id, Trade.version == version)
        return db.execute(stmt).scalars().first()

    @staticmethod
    def get_latest_trade_version(db: Session, trade_id: str) -> Optional[Trade]:
        """Get the latest version of a trade"""
        stmt = select(Trade).where(Trade.trade_id == trade_id).order_by(Trade.version.desc())
        return db.execute(stmt).scalars().first()

    @staticmethod
    def create_trade(db: Session, trade: TradeCreate, trace_id: Optional[str] = None) -> Trade:
        """
        Create a new trade with validations:
        1. Reject trades with lower version than existing
        2. Replace trades with same version
        3. Reject trades with maturity date in the past
        """
        # Check for existing trade with same trade_id
        logger.info(
            "%sCreate trade requested: trade_id=%s version=%s",
            TradeService._ctx(trace_id),
            trade.trade_id,
            trade.version,
        )

        latest_trade = TradeService.get_latest_trade_version(db, trade.trade_id)

        if latest_trade:
            # Validation 1: Reject lower version
            if trade.version < latest_trade.version:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Trade version {trade.version} is lower than existing version {latest_trade.version}",
                )

            # Same version: Replace existing trade
            if trade.version == latest_trade.version:
                delete_stmt = delete(Trade).where(
                    Trade.trade_id == latest_trade.trade_id,
                    Trade.version == latest_trade.version,
                )
                db.execute(delete_stmt)

        # Create new trade
        db.execute(insert(Trade).values(**trade.model_dump()))
        db.commit()
        created_trade = TradeService.get_trade(db, trade.trade_id, trade.version)
        if not created_trade:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Trade was created but could not be retrieved",
            )
        logger.info("%sCreate trade committed: trade_id=%s version=%s", TradeService._ctx(trace_id), trade.trade_id, trade.version)
        return created_trade

    @staticmethod
    def update_trade(db: Session, trade_id: str, version: int, trade_update: TradeUpdate, trace_id: Optional[str] = None) -> Trade:
        logger.info("%sUpdate trade requested: trade_id=%s version=%s", TradeService._ctx(trace_id), trade_id, version)
        db_trade = TradeService.get_trade(db, trade_id, version)
        if not db_trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Trade {trade_id} with version {version} not found"
            )

        update_data = trade_update.model_dump(exclude_unset=True)

        if db_trade.expired:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot update an expired trade",
            )

        if update_data:
            update_stmt = update(Trade).where(Trade.trade_id == trade_id, Trade.version == version).values(**update_data)
            db.execute(update_stmt)

        db.commit()
        logger.info("%sUpdate trade committed: trade_id=%s version=%s", TradeService._ctx(trace_id), trade_id, version)
        updated_trade = TradeService.get_trade(db, trade_id, version)
        if not updated_trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Trade {trade_id} with version {version} not found"
            )
        return updated_trade

    @staticmethod
    def delete_trade(db: Session, trade_id: str, version: int, trace_id: Optional[str] = None) -> bool:
        logger.info("%sDelete trade requested: trade_id=%s version=%s", TradeService._ctx(trace_id), trade_id, version)
        db_trade = TradeService.get_trade(db, trade_id, version)
        if not db_trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Trade {trade_id} with version {version} not found"
            )

        delete_stmt = delete(Trade).where(Trade.trade_id == trade_id, Trade.version == version)
        db.execute(delete_stmt)
        db.commit()
        logger.info("%sDelete trade committed: trade_id=%s version=%s", TradeService._ctx(trace_id), trade_id, version)
        return True

    @staticmethod
    def mark_expired_trades(db: Session) -> int:
        """
        Mark all trades as expired where maturity_date is before today
        """
        today = date.today()
        update_stmt = (
            update(Trade)
            .where(func.date(Trade.maturity_date) < today, Trade.expired.is_(False))
            .values(expired=True)
        )
        result = db.execute(update_stmt)
        db.commit()
        return result.rowcount or 0
