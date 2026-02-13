from pydantic import BaseModel, Field, field_validator
from datetime import date
from typing import Optional


class TradeBase(BaseModel):
    """Base trade schema"""
    trade_id: str = Field(..., description="Unique trade identifier")
    version: int = Field(..., gt=0, description="Trade version (must be positive)")
    counter_party_id: str = Field(..., description="Counter party identifier")
    book_id: str = Field(..., description="Book identifier")
    maturity_date: date = Field(..., description="Trade maturity date")
    created_date: date = Field(..., description="Trade creation date")
    expired: bool = Field(default=False, description="Expiry status")

    @field_validator('maturity_date')
    @classmethod
    def validate_maturity_date(cls, v: date) -> date:
        if v < date.today():
            raise ValueError(f"Maturity date {v} cannot be before today's date {date.today()}")
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
    maturity_date: Optional[date] = None
    created_date: Optional[date] = None
    expired: Optional[bool] = None

    @field_validator('maturity_date')
    @classmethod
    def validate_maturity_date(cls, v: Optional[date]) -> Optional[date]:
        """Validate that maturity date is not in the past"""
        if v is not None and v < date.today():
            raise ValueError(f"Maturity date {v} cannot be before today's date {date.today()}")
        return v
