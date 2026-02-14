# Trade Store API - Setup Instructions

## Quick Setup Guide

### 1. Environment Setup

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup

**Option A: Using Docker Compose (Recommended)**
```powershell
docker-compose up -d
```

**Option B: Local PostgreSQL**
```powershell
# Connect to PostgreSQL
psql -U postgres

# Create database and user
CREATE DATABASE tradedb;
CREATE USER tradeuser WITH PASSWORD 'tradepass';
GRANT ALL PRIVILEGES ON DATABASE tradedb TO tradeuser;
\q
```

### 3. Environment Variables

```powershell
# Set variables in current PowerShell session (local run)
$env:DATABASE_URL = "postgresql://tradeuser:tradepass@localhost:5432/tradedb"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_TOPIC = "trades"
$env:KAFKA_GROUP_ID = "trade-consumer-group"
```

### 3.1 GitHub Actions Variables/Secrets

Set these in GitHub repository settings (`Settings > Secrets and variables > Actions`):

- **Secrets**: `DATABASE_URL`, `POSTGRES_PASSWORD`
- **Variables**: `POSTGRES_USER`, `POSTGRES_DB`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`

### 4. Run Application

```powershell
# Start the API
uvicorn app.main:app --reload

# API will be available at:
# - http://localhost:8000
# - Docs: http://localhost:8000/docs
```

### 5. Run Tests

```powershell
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=app --cov-report=html

# View coverage report
start htmlcov/index.html
```

### 6. GitHub Setup

```powershell
# Initialize git repository
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: Trade Store API"

# Add remote (replace YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/trade_assignment.git

# Create and push to main branch
git branch -M main
git push -u origin main
```

### 7. Testing the API

**Using PowerShell:**
```powershell
# Create a trade
$body = @{
    trade_id = "T1"
    version = 1
    counter_party_id = "CP-1"
    book_id = "B1"
    maturity_date = "2026-05-20"
    created_date = "2026-02-10"
    expired = $false
} | ConvertTo-Json

Invoke-RestMethod -Uri http://localhost:8000/trades -Method Post -Body $body -ContentType "application/json"

# Get all trades
Invoke-RestMethod -Uri http://localhost:8000/trades -Method Get
```

**Using curl:**
```bash
# Create a trade
curl -X POST "http://localhost:8000/trades" -H "Content-Type: application/json" -d "{\"trade_id\":\"T1\",\"version\":1,\"counter_party_id\":\"CP-1\",\"book_id\":\"B1\",\"maturity_date\":\"2026-05-20\",\"created_date\":\"2026-02-10\",\"expired\":false}"

# Get all trades
curl http://localhost:8000/trades
```

### 8. Security Scanning

```powershell
# Install security tools
pip install safety bandit

# Run dependency scan
safety check

# Run code security scan
bandit -r app/ -ll
```

## Validation Testing

Test the three main validations:

### 1. Version Control Test
```powershell
# Create initial trade (version 2)
$trade_v2 = @{
    trade_id = "T1"
    version = 2
    counter_party_id = "CP-1"
    book_id = "B1"
    maturity_date = "2026-05-20"
    created_date = "2026-02-10"
    expired = $false
} | ConvertTo-Json

Invoke-RestMethod -Uri http://localhost:8000/trades -Method Post -Body $trade_v2 -ContentType "application/json"

# Try lower version (should fail)
$trade_v1 = $trade_v2 | ConvertFrom-Json
$trade_v1.version = 1
$trade_v1_json = $trade_v1 | ConvertTo-Json

Invoke-RestMethod -Uri http://localhost:8000/trades -Method Post -Body $trade_v1_json -ContentType "application/json"
# Expected: 400 Bad Request
```

### 2. Maturity Date Validation
```powershell
# Try past maturity date (should fail)
$past_trade = @{
    trade_id = "T2"
    version = 1
    counter_party_id = "CP-2"
    book_id = "B1"
    maturity_date = "2020-05-20"
    created_date = "2026-02-10"
    expired = $false
} | ConvertTo-Json

Invoke-RestMethod -Uri http://localhost:8000/trades -Method Post -Body $past_trade -ContentType "application/json"
# Expected: 422 Validation Error
```

### 3. Auto-Expiry Test
```powershell
# Trigger manual expiry check
Invoke-RestMethod -Uri http://localhost:8000/trades/expire -Method Post
```

## Docker Commands

```powershell
# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Rebuild
docker-compose up -d --build

# Check status
docker-compose ps
```

## Troubleshooting

### Port Already in Use
```powershell
# Find process using port 8000
netstat -ano | findstr :8000

# Kill the process (replace PID)
taskkill /PID <PID> /F
```

### Database Connection Issues
```powershell
# Check PostgreSQL is running
docker-compose ps postgres

# Restart PostgreSQL
docker-compose restart postgres

# View PostgreSQL logs
docker-compose logs postgres
```

### Virtual Environment Issues
```powershell
# Deactivate
deactivate

# Remove and recreate
Remove-Item -Recurse -Force venv
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Next Steps

1.  Review API documentation at http://localhost:8000/docs
2.  Run all tests: `pytest tests/ -v`
3.  Push code to GitHub
4.  Verify GitHub Actions pipeline runs successfully
5.  View PlantUML diagrams in `diagrams/` folder
6.  Customize deployment in `.github/workflows/ci.yml`

## Additional Resources

- FastAPI Documentation: https://fastapi.tiangolo.com/
- SQLAlchemy Documentation: https://docs.sqlalchemy.org/
- pytest Documentation: https://docs.pytest.org/
- PlantUML Documentation: https://plantuml.com/
