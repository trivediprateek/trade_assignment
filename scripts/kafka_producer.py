"""
Kafka Producer Script for Testing Trade Store API

This script sends sample trade messages to the Kafka topic for testing
the Kafka consumer integration.

Usage:
    # Send 10 sample trades
    python scripts/kafka_producer.py --count 10
    
    # Send trades continuously (stress test)
    python scripts/kafka_producer.py --continuous
    
    # Send from a JSON file
    python scripts/kafka_producer.py --file trades.json

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9093)
    KAFKA_TOPIC: Topic to produce to (default: trades)
"""

import argparse
import json
import logging
import os
import time
from datetime import date, timedelta
from typing import List, Dict

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_kafka_config() -> dict:
    """Get Kafka producer configuration"""
    return {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093'),
        'client.id': 'trade-producer',
    }


def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        logger.error(f' Message delivery failed: {err}')
    else:
        logger.info(
            f' Message delivered to {msg.topic()} '
            f'[partition {msg.partition()}] at offset {msg.offset()}'
        )


def create_topic_if_not_exists(topic: str):
    """Create Kafka topic if it doesn't exist"""
    admin_client = AdminClient(get_kafka_config())
    
    # Check if topic exists
    metadata = admin_client.list_topics(timeout=10)
    if topic in metadata.topics:
        logger.info(f"Topic '{topic}' already exists")
        return
    
    # Create topic with multiple partitions for scalability
    # Same trade_id always goes to same partition (preserves order per trade)
    logger.info(f"Creating topic '{topic}'...")
    new_topic = NewTopic(
        topic,
        num_partitions=3,  # Multiple partitions for parallel processing
        replication_factor=1
    )
    
    fs = admin_client.create_topics([new_topic])
    for topic_name, f in fs.items():
        try:
            f.result()  # Wait for operation to complete
            logger.info(f" Topic '{topic_name}' created successfully")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")


def generate_sample_trade(trade_id: str, version: int, operation: str = "CREATE") -> Dict:
    """
    Generate a sample trade message with operation type
    
    Args:
        trade_id: Trade identifier
        version: Trade version
        operation: Operation type - "CREATE", "UPDATE", or "DELETE"
    
    Returns:
        Message dict with operation and data
    """
    data = {
        "trade_id": trade_id,
        "version": version,
    }
    
    # For DELETE, only need trade_id and version
    if operation != "DELETE":
        data.update({
            "counter_party_id": f"CP-{trade_id[-3:]}",
            "book_id": f"BOOK-{version % 5 + 1}",
            "maturity_date": str(date.today() + timedelta(days=30 + version)),
            "created_date": str(date.today()),
            "expired": False
        })
    
    return {
        "operation": operation,
        "data": data
    }


def send_sample_trades(count: int):
    """Send multiple sample trades to Kafka"""
    topic = os.getenv('KAFKA_TOPIC', 'trades')
    config = get_kafka_config()
    
    # Create topic if needed
    create_topic_if_not_exists(topic)
    
    # Create producer
    producer = Producer(config)
    
    logger.info(f"Sending {count} sample trades to topic '{topic}'...")
    logger.info(f"Bootstrap servers: {config['bootstrap.servers']}")
    
    sent_count = 0
    
    try:
        for i in range(1, count + 1):
            # Generate trade
            trade_id = f"T{i:04d}"
            version = 1
            # Generate message with CREATE operation
            message_data = generate_sample_trade(trade_id, version, operation="CREATE")
            
            # Serialize to JSON
            message = json.dumps(message_data)
            
            # Send to Kafka (asynchronous)
            producer.produce(
                topic,
                value=message.encode('utf-8'),
                key=trade_id.encode('utf-8'),
                callback=delivery_report
            )
            
            sent_count += 1
            
            # Trigger delivery report callbacks
            producer.poll(0)
            
            # Progress logging
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{count} trades sent")
                producer.flush()  # Ensure delivery
        
        # Wait for all messages to be delivered
        logger.info("Waiting for all messages to be delivered...")
        producer.flush()
        
        logger.info(f"\n Successfully sent {sent_count} trades!")
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        producer.flush()


def send_continuous_trades(delay: float = 0.1):
    """Send trades continuously for stress testing"""
    topic = os.getenv('KAFKA_TOPIC', 'trades')
    config = get_kafka_config()
    
    create_topic_if_not_exists(topic)
    producer = Producer(config)
    
    logger.info(f"Sending trades continuously to '{topic}' (Ctrl+C to stop)...")
    logger.info(f"Delay between messages: {delay}s")
    
    trade_counter = 1
    
    try:
        while True:
            trade_id = f"T{trade_counter:06d}"
            version = 1
            # Generate message with CREATE operation
            message_data = generate_sample_trade(trade_id, version, operation="CREATE")
            
            message = json.dumps(message_data)
            producer.produce(
                topic,
                value=message.encode('utf-8'),
                key=trade_id.encode('utf-8')
            )
            
            producer.poll(0)
            
            if trade_counter % 100 == 0:
                logger.info(f"Sent {trade_counter} trades...")
                producer.flush()
            
            trade_counter += 1
            time.sleep(delay)
            
    except KeyboardInterrupt:
        logger.info(f"\n Stopped after sending {trade_counter - 1} trades")
    finally:
        producer.flush()


def send_from_file(filepath: str):
    """Send trades from a JSON file"""
    topic = os.getenv('KAFKA_TOPIC', 'trades')
    config = get_kafka_config()
    
    create_topic_if_not_exists(topic)
    producer = Producer(config)
    
    logger.info(f"Reading trades from '{filepath}'...")
    
    try:
        with open(filepath, 'r') as f:
            trades = json.load(f)
        
        if not isinstance(trades, list):
            trades = [trades]
        
        logger.info(f"Found {len(trades)} trades in file")
        
        for i, trade_data in enumerate(trades, 1):
            message = json.dumps(trade_data)
            trade_id = trade_data.get('trade_id', f'T{i}')
            
            producer.produce(
                topic,
                value=message.encode('utf-8'),
                key=trade_id.encode('utf-8'),
                callback=delivery_report
            )
            
            producer.poll(0)
        
        producer.flush()
        logger.info(f" Successfully sent {len(trades)} trades from file!")
        
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file: {e}")
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Send sample trades to Kafka for testing'
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--count',
        type=int,
        help='Number of sample trades to send'
    )
    group.add_argument(
        '--continuous',
        action='store_true',
        help='Send trades continuously (stress test)'
    )
    group.add_argument(
        '--file',
        type=str,
        help='Send trades from JSON file'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay between messages in continuous mode (seconds)'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Trade Store Kafka Producer")
    logger.info("=" * 60)
    
    if args.count:
        send_sample_trades(args.count)
    elif args.continuous:
        send_continuous_trades(args.delay)
    elif args.file:
        send_from_file(args.file)


if __name__ == "__main__":
    main()
