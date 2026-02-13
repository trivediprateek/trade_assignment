from sqlalchemy import Column, String, Integer, Date, Boolean
from app.database import Base


class Trade(Base):
    """Trade model representing a trade in the database"""
    __tablename__ = "trades"

    trade_id = Column(String, primary_key=True, index=True)
    version = Column(Integer, primary_key=True)
    counter_party_id = Column(String, nullable=False)
    book_id = Column(String, nullable=False)
    maturity_date = Column(Date, nullable=False)
    created_date = Column(Date, nullable=False)
    expired = Column(Boolean, default=False, nullable=False)

    def __repr__(self):
        return f"<Trade(trade_id={self.trade_id}, version={self.version})>"
