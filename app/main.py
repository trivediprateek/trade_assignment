from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing import List
from contextlib import asynccontextmanager
import json
import os

from confluent_kafka import Producer
from app.database import engine, get_db, Base
from app.schemas import TradeCreate, TradeResponse, TradeUpdate
from app.crud import TradeService
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Create database tables (skip if testing)
if os.getenv("TESTING") != "1":
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Warning: Could not create database tables: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events for the application"""
    # Startup: Initialize Kafka producer
    kafka_config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": "trade-api-producer",
    }
    app.state.kafka_producer = Producer(kafka_config)
    print(f" Kafka producer initialized: {kafka_config['bootstrap.servers']}")

    # Startup: Mark expired trades on startup (skip in test mode)
    if os.getenv("TESTING") != "1":
        db = next(get_db())
        try:
            count = TradeService.mark_expired_trades(db)
            print(f"Marked {count} trades as expired on startup")
        finally:
            db.close()

    if os.getenv("TESTING") != "1":
        # Startup: Initialize background scheduler for auto-expiry
        scheduler = AsyncIOScheduler()

        def check_expired_trades():
            db = next(get_db())
            try:
                count = TradeService.mark_expired_trades(db)
                if count > 0:
                    print(f" Auto-marked {count} trades as expired")
            finally:
                db.close()

        # Run expiry check every hour
        scheduler.add_job(check_expired_trades, "interval", hours=1)
        scheduler.start()
        print(" Background scheduler started")

        # Run once on startup
        check_expired_trades()
    yield

    # Shutdown: Flush and close Kafka producer
    if hasattr(app.state, "kafka_producer") and app.state.kafka_producer:
        app.state.kafka_producer.flush()
        print(" Kafka producer flushed and closed")
    print("Application shutting down")


app = FastAPI(
    title="Trade Store API",
    description="REST API for managing trades with validation and auto-expiry",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", tags=["Health"])
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "Trade Store API is running"}


@app.post("/trades", status_code=status.HTTP_202_ACCEPTED, tags=["Trades"])
def create_trade(trade: TradeCreate, db: Session = Depends(get_db)):
    """
    Submit a new trade for processing.
    """
    if not hasattr(app.state, "kafka_producer") or app.state.kafka_producer is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Kafka producer not available")

    latest_trade = TradeService.get_latest_trade_version(db, trade.trade_id)
    if latest_trade and trade.version < latest_trade.version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Trade version {trade.version} is lower than existing version {latest_trade.version}",
        )

    # Convert Pydantic model to dict and then to JSON
    trade_data = trade.model_dump(mode="json")
    message = json.dumps(trade_data)

    topic = os.getenv("KAFKA_TOPIC", "trades")

    try:
        app.state.kafka_producer.produce(
            topic,
            value=message.encode("utf-8"),
            key=trade.trade_id.encode("utf-8"),
            callback=lambda err, msg: print(f" Kafka error: {err}") if err else None,
        )
        app.state.kafka_producer.poll(0)  # Trigger delivery callbacks

        return {
            "operation": "CREATE",
            "status": "accepted",
            "message": f"Trade {trade.trade_id} v{trade.version} queued for processing",
            "trade_id": trade.trade_id,
            "version": trade.version,
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to queue trade: {str(e)}"
        )


@app.put("/trades/{trade_id}/{version}", status_code=status.HTTP_202_ACCEPTED, tags=["Trades"])
def update_trade(trade_id: str, version: int, trade_update: TradeUpdate):
    if not hasattr(app.state, "kafka_producer") or app.state.kafka_producer is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Kafka producer not available")

    message_data = {
        "operation": "UPDATE",
        "data": {"trade_id": trade_id, "version": version, **trade_update.model_dump(exclude_unset=True, mode="json")},
    }
    message = json.dumps(message_data)

    topic = os.getenv("KAFKA_TOPIC", "trades")

    # Send to Kafka
    try:
        app.state.kafka_producer.produce(
            topic,
            value=message.encode("utf-8"),
            key=trade_id.encode("utf-8"),
            callback=lambda err, msg: print(f" Kafka error: {err}") if err else None,
        )
        app.state.kafka_producer.poll(0)

        return {
            "status": "accepted",
            "message": f"Update for trade {trade_id} v{version} queued for processing",
            "trade_id": trade_id,
            "version": version,
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to queue update: {str(e)}"
        )


@app.delete("/trades/{trade_id}/{version}", status_code=status.HTTP_202_ACCEPTED, tags=["Trades"])
def delete_trade(trade_id: str, version: int):
    """
    Submit a trade deletion for processing.

    The deletion is sent to Kafka and will be processed asynchronously by the consumer.

    Returns: 202 Accepted with message that deletion is queued for processing
    """
    if not hasattr(app.state, "kafka_producer") or app.state.kafka_producer is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Kafka producer not available")

    # Build message with operation type
    message_data = {"operation": "DELETE", "data": {"trade_id": trade_id, "version": version}}
    message = json.dumps(message_data)

    # Get Kafka topic from environment
    topic = os.getenv("KAFKA_TOPIC", "trades")

    # Send to Kafka
    try:
        app.state.kafka_producer.produce(
            topic,
            value=message.encode("utf-8"),
            key=trade_id.encode("utf-8"),
            callback=lambda err, msg: print(f" Kafka error: {err}") if err else None,
        )
        app.state.kafka_producer.poll(0)

        return {
            "status": "accepted",
            "message": f"Delete for trade {trade_id} v{version} queued for processing",
            "trade_id": trade_id,
            "version": version,
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to queue deletion: {str(e)}"
        )


@app.get("/trades", response_model=List[TradeResponse], tags=["Trades"])
def get_all_trades(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    from app.models import Trade

    trades = db.execute(select(Trade).offset(skip).limit(limit)).scalars().all()
    return trades


@app.get("/trades/{trade_id}/{version}", response_model=TradeResponse, tags=["Trades"])
def get_trade(trade_id: str, version: int, db: Session = Depends(get_db)):
    """Get specific trade by ID and version"""
    trade = TradeService.get_trade(db, trade_id, version)
    if not trade:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Trade {trade_id} v{version} not found")
    return trade


@app.get("/trades/{trade_id}/latest", response_model=TradeResponse, tags=["Trades"])
def get_latest_trade(trade_id: str, db: Session = Depends(get_db)):
    """Get latest version of a trade"""
    trade = TradeService.get_latest_trade_version(db, trade_id)
    if not trade:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Trade {trade_id} not found")
    return trade


@app.post("/trades/expire", tags=["Trades"])
def mark_expired_trades(db: Session = Depends(get_db)):
    """Manually trigger expiry check for all trades"""
    count = TradeService.mark_expired_trades(db)
    return {"message": f"Marked {count} trades as expired", "count": count}
