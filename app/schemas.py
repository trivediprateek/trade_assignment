from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date
from typing import Optional


class TradeBase(BaseModel):
    """Base trade schema"""

    trade_id: str = Field(..., description="Unique trade identifier")
    version: int = Field(..., gt=0, description="Trade version (must be positive)")
    counter_party_id: str = Field(..., description="Counter party identifier")
    book_id: str = Field(..., description="Book identifier")
    maturity_date: datetime = Field(..., description="Trade maturity datetime")
    created_date: datetime = Field(..., description="Trade creation datetime")
    expired: bool = Field(default=False, description="Expiry status")

    @field_validator("maturity_date")
    @classmethod
    def validate_maturity_date(cls, v: datetime) -> datetime:
        if v.date() < date.today():
            raise ValueError(f"Maturity datetime {v} cannot be before today's date {date.today()}")
        return v


class TradeCreate(TradeBase):
    """Schema for creating a trade"""

    pass


class TradeResponse(TradeBase):
    """Schema for trade response"""

    class Config:
        from_attributes = True


class TradeUpdate(BaseModel):
    """Schema for updating a trade"""

    counter_party_id: Optional[str] = None
    book_id: Optional[str] = None
    maturity_date: Optional[datetime] = None
    created_date: Optional[datetime] = None
    expired: Optional[bool] = None

    @field_validator("maturity_date")
    @classmethod
    def validate_maturity_date(cls, v: Optional[datetime]) -> Optional[datetime]:
        """Validate that maturity date is not before today"""
        if v is not None and v.date() < date.today():
            raise ValueError(f"Maturity datetime {v} cannot be before today's date {date.today()}")
        return v
