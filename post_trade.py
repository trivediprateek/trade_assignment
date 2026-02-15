import requests, json

data = {
    "trade_id": "T0005", "version": 4, 
    "counter_party_id": "CP-2", 
    "book_id": "B200", 
    "maturity_date": "2026-03-08T00:00:00Z", 
    "created_date": "2024-06-01T00:00:00Z"
}
response = requests.post("http://localhost:8000/trades", json=data)
print(response.status_code, response.text)


