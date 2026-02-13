from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models import Trade
from app.schemas import TradeCreate, TradeUpdate
from datetime import date
from typing import List, Optional
from fastapi import HTTPException, status


class TradeService:
    """Service layer for trade operations"""

    @staticmethod
    def get_trade(db: Session, trade_id: str, version: int) -> Optional[Trade]:
        """Get a specific trade by ID and version"""
        return db.query(Trade).filter(
            and_(Trade.trade_id == trade_id, Trade.version == version)
        ).first()

    @staticmethod
    def get_latest_trade_version(db: Session, trade_id: str) -> Optional[Trade]:
        """Get the latest version of a trade"""
        return db.query(Trade).filter(
            Trade.trade_id == trade_id
        ).order_by(Trade.version.desc()).first()

    @staticmethod
    def get_all_trades(db: Session, skip: int = 0, limit: int = 100) -> List[Trade]:
        """Get all trades with pagination"""
        return db.query(Trade).offset(skip).limit(limit).all()

    @staticmethod
    def create_trade(db: Session, trade: TradeCreate) -> Trade:
        """
        Create a new trade with validations:
        1. Reject trades with lower version than existing
        2. Replace trades with same version
        3. Reject trades with maturity date in the past
        """
        # Check for existing trade with same trade_id
        latest_trade = TradeService.get_latest_trade_version(db, trade.trade_id)
        
        if latest_trade:
            # Validation 1: Reject lower version
            if trade.version < latest_trade.version:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Trade version {trade.version} is lower than existing version {latest_trade.version}"
                )
            
            # Same version: Replace existing trade
            if trade.version == latest_trade.version:
                # Delete existing trade with same version
                db.delete(latest_trade)
                db.commit()

        # Create new trade
        db_trade = Trade(**trade.model_dump())
        db.add(db_trade)
        db.commit()
        db.refresh(db_trade)
        return db_trade

    @staticmethod
    def update_trade(db: Session, trade_id: str, version: int, trade_update: TradeUpdate) -> Trade:
        """Update an existing trade"""
        db_trade = TradeService.get_trade(db, trade_id, version)
        if not db_trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trade {trade_id} with version {version} not found"
            )
        
        update_data = trade_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_trade, field, value)
        
        db.commit()
        db.refresh(db_trade)
        return db_trade

    @staticmethod
    def delete_trade(db: Session, trade_id: str, version: int) -> bool:
        """Delete a trade"""
        db_trade = TradeService.get_trade(db, trade_id, version)
        if not db_trade:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trade {trade_id} with version {version} not found"
            )
        
        db.delete(db_trade)
        db.commit()
        return True

    @staticmethod
    def mark_expired_trades(db: Session) -> int:
        """
        Mark all trades as expired where maturity_date < today
        Returns the number of trades marked as expired
        """
        today = date.today()
        expired_trades = db.query(Trade).filter(
            and_(Trade.maturity_date < today, Trade.expired == False)
        ).all()
        
        count = 0
        for trade in expired_trades:
            trade.expired = True
            count += 1
        
        db.commit()
        return count
