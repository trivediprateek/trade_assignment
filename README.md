# Trade Store REST API

[![CI/CD Pipeline](https://github.com/YOUR_USERNAME/backend_assignment/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/backend_assignment/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/YOUR_USERNAME/backend_assignment/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/backend_assignment)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109.0-green.svg)](https://fastapi.tiangolo.com/)
[![Kafka](https://img.shields.io/badge/Kafka-Enabled-orange.svg)](https://kafka.apache.org/)

A robust REST API for managing financial trades with comprehensive validation, automated expiry handling, and high-volume Kafka streaming support. Built with Test-Driven Development (TDD) principles.

## 🎯 Features

- **Dual Transmission Methods**:
  - **REST API** for manual operations and queries
  - **Kafka Streaming** for high-volume automated trade feeds (thousands/second)
- **PostgreSQL Database** for persistent storage
- **Comprehensive Validation**:
  - Version control (rejects lower versions, replaces same versions)
  - Maturity date validation (rejects past dates)
  - Automatic expiry marking for matured trades
- **91% Test Coverage** with pytest
- **CI/CD Pipeline** with GitHub Actions
- **Security Scanning** (Safety + Bandit)
- **Docker Support** with Kafka cluster
- **PlantUML Diagrams** for architecture visualization

## 📋 Business Requirements

### Trade Validations

1. **Version Control**: 
   - Trades with lower versions than existing are rejected
   - Trades with the same version replace the current record
   - Trades with higher versions are accepted

2. **Maturity Date Validation**:
   - Trades with maturity dates in the past are rejected
   - Only future or current dates are accepted

3. **Auto-Expiry**:
   - Trades automatically marked as expired when maturity date passes
   - Background tasks handle expiry checks

## 🏗️ Architecture

### High-Volume Trade Processing

```
Trading Systems → Kafka Topic → Consumer Service → PostgreSQL
(producers)       (buffer)      (validation)      (storage)
                                      ↕
                                  REST API ← Manual Operations
```

**Two Transmission Methods**:
1. **Kafka Streaming** - For high-volume automated feeds (thousands/second)
2. **REST API** - For manual operations, queries, admin tasks

### Component Diagram

```
┌─────────────┐
│   Client    │ (HTTP Requests)
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  FastAPI Endpoints  │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Trade Service     │  ◄──── Business Logic & Validations
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  SQLAlchemy ORM     │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   PostgreSQL DB     │ ◄──── Also accessed by Kafka Consumer
└─────────────────────┘
       ▲
       │
┌──────┴──────────────┐
│  Kafka Consumer     │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Kafka Cluster     │ ◄──── High-volume trade feed
└─────────────────────┘
```

See [diagrams/](diagrams/) folder for detailed PlantUML diagrams.

**📚 Kafka Integration**: See [KAFKA_SETUP.md](KAFKA_SETUP.md) for complete streaming setup guide.

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+ (or use Docker Compose)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/backend_assignment.git
   cd backend_assignment
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   # Windows
   venv\Scripts\activate
   # Linux/Mac
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   copy .env.example .env
   # Edit .env with your database credentials
   ```

5. **Run with Docker Compose** (Recommended - includes Kafka)
   ```bash
   docker-compose up -d
   ```
   
   This starts:
   - PostgreSQL (port 5432)
   - Kafka + Zookeeper (ports 9092/9093)
   - FastAPI REST API (port 8000)
   - Kafka Consumer (background service)
   
   The API will be available at `http://localhost:8000`

6. **Send test trades via Kafka** (Optional)
   ```bash
   python scripts/kafka_producer.py --count 100
   ```

### Manual Setup (Without Docker)

1. **Start PostgreSQL** and create database:
   ```sql
   CREATE DATABASE tradedb;
   CREATE USER tradeuser WITH PASSWORD 'tradepass';
   GRANT ALL PRIVILEGES ON DATABASE tradedb TO tradeuser;
   ```

2. **Initialize database tables**
   ```bash
   python scripts/init_db.py
   ```
   
   This creates the `trades` table. You should see:
   ```
    Tables created successfully:
     - trades
       Columns: trade_id, version, counter_party_id, book_id, maturity_date, created_date, expired
   ```

3. **Run the application**
   ```bash
   uvicorn app.main:app --reload
   ```

4. **Access the API**
   - API: http://localhost:8000
   - Interactive Docs: http://localhost:8000/docs
   - ReDoc: http://localhost:8000/redoc

## 📚 API Endpoints

### Trades

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Health check |
| `POST` | `/trades` | Create a new trade |
| `GET` | `/trades` | Get all trades (with pagination) |
| `GET` | `/trades/{trade_id}/{version}` | Get specific trade |
| `GET` | `/trades/{trade_id}/latest` | Get latest version of a trade |
| `PUT` | `/trades/{trade_id}/{version}` | Update a trade |
| `DELETE` | `/trades/{trade_id}/{version}` | Delete a trade |
| `POST` | `/trades/expire` | Manually trigger expiry check |

### Example: Create Trade

```bash
curl -X POST "http://localhost:8000/trades" \
  -H "Content-Type: application/json" \
  -d '{
    "trade_id": "T1",
    "version": 1,
    "counter_party_id": "CP-1",
    "book_id": "B1",
    "maturity_date": "2026-05-20",
    "created_date": "2026-02-10",
    "expired": false
  }'
```

### Example Response

```json
{
  "trade_id": "T1",
  "version": 1,
  "counter_party_id": "CP-1",
  "book_id": "B1",
  "maturity_date": "2026-05-20",
  "created_date": "2026-02-10",
  "expired": false
}
```

## 🧪 Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ -v --cov=app --cov-report=html
```

### View Coverage Report

```bash
# Windows
start htmlcov/index.html
# Linux/Mac
open htmlcov/index.html
```

### Test Structure

```
tests/
├── __init__.py
├── conftest.py             # Fixtures and test configuration
├── test_trades.py          # REST API tests (22 tests)
└── test_kafka_consumer.py  # Kafka integration tests (15 tests)
```

**Total Test Coverage**: 91% across 37 tests

## 🔒 Security

### Vulnerability Scanning

The CI/CD pipeline includes automated security scanning:

- **Safety**: Scans Python dependencies for known vulnerabilities
- **Bandit**: Static code analysis for security issues

### Run Security Scans Locally

```bash
# Dependency scan
safety check

# Code security scan
bandit -r app/ -ll
```

## 🔄 CI/CD Pipeline

GitHub Actions workflow includes:

1. **Testing**: Automated unit tests with PostgreSQL service
2. **Security Scan**: Dependency and code vulnerability checks
3. **Code Quality**: Linting with flake8, formatting checks
4. **Build**: Docker image build and validation
5. **Deploy**: Automated deployment on main branch

### Pipeline Stages

```
┌──────────┐    ┌──────────────┐    ┌──────┐    ┌───────┐    ┌────────┐
│   Test   │───▶│Security Scan │───▶│ Lint │───▶│ Build │───▶│ Deploy │
└──────────┘    └──────────────┘    └──────┘    └───────┘    └────────┘
```

Pipeline **fails** if:
- Any test fails
- Critical/High security vulnerabilities detected
- Docker build fails

## 📊 Database Schema

```sql
CREATE TABLE trades (
    trade_id VARCHAR PRIMARY KEY,
    version INTEGER PRIMARY KEY,
    counter_party_id VARCHAR NOT NULL,
    book_id VARCHAR NOT NULL,
    maturity_date DATE NOT NULL,
    created_date DATE NOT NULL,
    expired BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (trade_id, version)
);
```

### Database Management

**Initialize Database** (First-time setup):
```bash
python scripts/init_db.py
```

**Reset Database** (  Deletes all data):
```bash
python scripts/init_db.py --drop
```

**Drop Tables Only**:
```bash
python scripts/init_db.py --drop-only
```

**When are tables created?**
1. **Automatically**: When running the FastAPI app ([app/main.py](app/main.py#L13-L17))
2. **Manually**: Using `scripts/init_db.py` (recommended for production)
3. **Tests**: Automatically in test fixtures ([tests/conftest.py](tests/conftest.py#L36))

## 🎨 PlantUML Diagrams

Diagrams are located in the `diagrams/` folder:

- **architecture.puml**: System component diagram
- **sequence_create_trade.puml**: Trade creation flow
- **class_diagram.puml**: Class relationships
- **expiry_process.puml**: Auto-expiry activity diagram

### Viewing Diagrams

Use PlantUML viewer or online tools:
- [PlantUML Online Editor](http://www.plantuml.com/plantuml/uml/)
- VS Code PlantUML Extension

## 📁 Project Structure

```
trade_assignment/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application
│   ├── models.py            # SQLAlchemy models
│   ├── schemas.py           # Pydantic schemas
│   ├── database.py          # Database configuration
│   ├── crud.py              # Business logic & CRUD operations
│   └── kafka_consumer.py    # Kafka consumer service
├── scripts/
│   ├── init_db.py           # Database initialization script ⭐
│   └── kafka_producer.py    # Kafka producer for testing
├── tests/
│   ├── __init__.py
│   ├── conftest.py          # Test fixtures
│   ├── test_trades.py       # REST API test cases
│   └── test_kafka_consumer.py  # Kafka tests
├── diagrams/
│   ├── architecture.puml
│   ├── class_diagram.puml
│   ├── sequence_create_trade.puml
│   └── expiry_process.puml
├── .github/
│   └── workflows/
│       └── ci.yml           # CI/CD pipeline
├── docker-compose.yml       # Includes Kafka cluster (UPDATED)
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## 🛠️ Development

### Code Style

- **Formatter**: Black
- **Import Sorting**: isort
- **Linter**: Flake8

```bash
# Format code
black app/ tests/

# Sort imports
isort app/ tests/

# Lint
flake8 app/ tests/ --max-line-length=120
```

## 🐛 Troubleshooting

### Database Connection Issues

```bash
# Check PostgreSQL is running
docker-compose ps

# View logs
docker-compose logs postgres

# Restart services
docker-compose restart
```

### Kafka Issues

```bash
# Check Kafka is running
docker-compose ps kafka

# View consumer logs
docker-compose logs -f kafka-consumer

# Send test message
python scripts/kafka_producer.py --count 1
```

See [KAFKA_SETUP.md](KAFKA_SETUP.md) for detailed Kafka troubleshooting.

### Test Failures

```bash
# Run specific test
pytest tests/test_trades.py::TestCreateTrade::test_create_trade_success -v

# Run with detailed output
pytest tests/ -vv -s
```

## 📝 Sample Test Data

```json
[
  {
    "trade_id": "T1",
    "version": 1,
    "counter_party_id": "CP-1",
    "book_id": "B1",
    "maturity_date": "2026-05-20",
    "created_date": "2026-02-10",
    "expired": false
  },
  {
    "trade_id": "T2",
    "version": 2,
    "counter_party_id": "CP-2",
    "book_id": "B1",
    "maturity_date": "2026-05-20",
    "created_date": "2026-02-10",
    "expired": false
  },
  {
    "trade_id": "T3",
    "version": 3,
    "counter_party_id": "CP-3",
    "book_id": "B2",
    "maturity_date": "2026-05-20",
    "created_date": "2026-02-10",
    "expired": false
  }
]
```

