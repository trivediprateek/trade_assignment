import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import os

# Set testing environment variable
os.environ["TESTING"] = "1"

from app.main import app
from app.database import Base, get_db

# Use in-memory SQLite for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """Override database dependency for testing"""
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


@pytest.fixture(scope="function")
def test_db():
    """Create fresh database for each test and return a session"""
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    yield db
    db.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def client(test_db):
    """Create test client with mocked Kafka producer"""
    app.dependency_overrides[get_db] = override_get_db

    # Mock Kafka producer to avoid actual Kafka calls in tests
    mock_producer = Mock()
    mock_producer.produce = Mock()
    mock_producer.poll = Mock()
    mock_producer.flush = Mock()

    # Set mock producer on app.state
    app.state.kafka_producer = mock_producer

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


@pytest.fixture
def sample_trade():
    """Sample trade data"""
    return {
        "trade_id": "T1",
        "version": 1,
        "counter_party_id": "CP-1",
        "book_id": "B1",
        "maturity_date": (datetime.now() + timedelta(days=365)).isoformat(),
        "created_date": datetime.now().isoformat(),
        "expired": False,
    }


@pytest.fixture
def past_maturity_trade():
    """Trade with past maturity date"""
    return {
        "trade_id": "T2",
        "version": 1,
        "counter_party_id": "CP-2",
        "book_id": "B1",
        "maturity_date": (datetime.now() - timedelta(days=1)).isoformat(),
        "created_date": datetime.now().isoformat(),
        "expired": False,
    }


@pytest.fixture
def incomplete_trade():
    return {"trade_id": "T1", "version": 1}
