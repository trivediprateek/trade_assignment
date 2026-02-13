"""
Kafka Consumer for Trade Store API

This module provides a Kafka consumer that continuously reads trade messages
from a Kafka topic and processes them using the same business logic as the REST API.

Usage:
    python -m app.kafka_consumer

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9093)
    KAFKA_TOPIC: Topic to consume from (default: trades)
    KAFKA_GROUP_ID: Consumer group ID (default: trade-consumer-group)
    DATABASE_URL: PostgreSQL connection string
"""

import json
import logging
import os
import signal
import sys
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.schemas import TradeCreate, TradeUpdate
from app.crud import TradeService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global running
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    running = False


def get_kafka_config() -> dict:
    """Get Kafka consumer configuration from environment variables"""
    return {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'group.id': os.getenv('KAFKA_GROUP_ID', 'trade-consumer-group'),
        'auto.offset.reset': 'earliest',  # Start from beginning if no offset
        'enable.auto.commit': False,  # Manual commit for reliability
        'max.poll.interval.ms': 300000,  # 5 minutes
    }


def process_trade_message(message_value: str, db: Session) -> bool:
    """
    Process a single trade message from Kafka based on operation
    """
    try:
        message = json.loads(message_value)

        operation = message.get('operation', 'CREATE')
        data = message.get('data', message)

        trade_id = data.get('trade_id')
        version = data.get('version')
        logger.info(f"Processing {operation}: {trade_id} v{version}")
        if operation == 'CREATE':
            # Validate data
            trade_create = TradeCreate(**data)
            
            # Create trade
            db_trade = TradeService.create_trade(db, trade_create)
            
            logger.info(
                f" Successfully created trade {db_trade.trade_id} "
                f"version {db_trade.version}"
            )
            return True
        elif operation == 'UPDATE':
            # Validate data
            trade_update = TradeUpdate(**data)
            
            db_trade = TradeService.update_trade(db, trade_id, version, trade_update)
            
            logger.info(
                f" Successfully updated trade {db_trade.trade_id} "
                f"version {db_trade.version}"
            )
            return True
            
        elif operation == 'DELETE':
            # Delete trade
            TradeService.delete_trade(db, trade_id, version)
            
            logger.info(
                f" Successfully deleted trade {trade_id} version {version}"
            )
            return True
            
        else:
            logger.error(f" Unknown operation: {operation}")
            return False
        
    except ValueError as e:
        # Pydantic validation error
        logger.error(f" Validation error: {e}")
        return False
        
    except Exception as e:
        # Business logic error (version conflict, not found, etc.)
        logger.error(f" Error processing trade: {e}")
        return False


def consume_trades():
    """
    Main consumer loop - continuously reads and processes trades from Kafka
    """
    topic = os.getenv('KAFKA_TOPIC', 'trades')
    config = get_kafka_config()
    
    logger.info(f"Starting Kafka consumer...")
    logger.info(f"  Bootstrap servers: {config['bootstrap.servers']}")
    logger.info(f"  Topic: {topic}")
    logger.info(f"  Group ID: {config['group.id']}")
    
    consumer = Consumer(config)
    
    try:
        # Subscribe to topic
        consumer.subscribe([topic])
        logger.info(f" Subscribed to topic '{topic}'")
        
        # Statistics
        processed_count = 0
        error_count = 0
        
        # Main consumption loop
        while running:
            # Poll for messages (1 second timeout)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue  # No message, keep polling
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition - normal, just continue
                    logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    # Actual error
                    logger.error(f"Kafka error: {msg.error()}")
                    error_count += 1
                continue
            
            # Get db session
            db = SessionLocal()
            try:
                # Process the message
                message_value = msg.value().decode('utf-8')
                success = process_trade_message(message_value, db)
                
                if success:
                    # Commit offset only after successful processing
                    consumer.commit(asynchronous=False)
                    processed_count += 1
                    
                    # Log progress every 100 trades
                    if processed_count % 100 == 0:
                        logger.info(f"Progress: {processed_count} trades processed")
                else:
                    error_count += 1
                    # Don't commit - message will be reprocessed
                    logger.warning(f"Skipping commit for failed message")
                    
            finally:
                db.close()
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except KafkaException as e:
        logger.error(f"Kafka exception: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        logger.info(f"\nShutdown summary:")
        logger.info(f"  Total processed: {processed_count}")
        logger.info(f"  Total errors: {error_count}")
        consumer.close()
        logger.info(" Consumer closed gracefully")


def main():
    """Entry point for Kafka consumer"""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("=" * 60)
    logger.info("Trade Store Kafka Consumer")
    logger.info("=" * 60)
    
    # Start consuming
    consume_trades()


if __name__ == "__main__":
    main()
