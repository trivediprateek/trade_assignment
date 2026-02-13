import pytest
from datetime import date, timedelta
from fastapi import status


class TestHealthCheck:
    """Test health check endpoint"""

    def test_health_check(self, client):
        """Test that health check endpoint returns 200"""
        response = client.get("/")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["status"] == "healthy"


class TestCreateTrade:
    """Test trade creation with validations"""

    def test_create_trade_success(self, client, sample_trade):
        """Test successful trade creation (async via Kafka)"""
        response = client.post("/trades", json=sample_trade)
        # With Kafka, we get 202 Accepted (async processing)
        assert response.status_code == status.HTTP_202_ACCEPTED
        data = response.json()
        assert data["status"] == "accepted"
        assert sample_trade["trade_id"] in data["message"]

    def test_create_trade_with_past_maturity_date_fails(self, client, past_maturity_trade):
        """Test validation: Reject trade with maturity date in the past"""
        response = client.post("/trades", json=past_maturity_trade)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert "maturity date" in response.json()["detail"][0]["msg"].lower()

    def test_create_trade_with_higher_version(self, client, sample_trade):
        """Test creating trade with higher version (async via Kafka)"""
        # Create initial trade
        response1 = client.post("/trades", json=sample_trade)
        assert response1.status_code == status.HTTP_202_ACCEPTED

        # Create trade with higher version
        higher_version_trade = sample_trade.copy()
        higher_version_trade["version"] = 2
        higher_version_trade["counter_party_id"] = "CP-2"
        
        response2 = client.post("/trades", json=higher_version_trade)
        # Both operations return 202 (async), actual version validation in consumer
        assert response2.status_code == status.HTTP_202_ACCEPTED
        assert higher_version_trade["trade_id"] in response2.json()["message"]

    def test_create_trade_with_lower_version_fails(self, client, sample_trade):
        """Test that lower version trade is still accepted but fails in consumer"""
        # Create initial trade with version 2
        sample_trade["version"] = 2
        response1 = client.post("/trades", json=sample_trade)
        assert response1.status_code == status.HTTP_202_ACCEPTED

        # Try to create trade with lower version (1)
        # API accepts it (202), but consumer will reject it
        lower_version_trade = sample_trade.copy()
        lower_version_trade["version"] = 1
        
        response2 = client.post("/trades", json=lower_version_trade)
        # With async processing, API returns 202 (validation happens in consumer)
        assert response2.status_code == status.HTTP_202_ACCEPTED

    def test_create_trade_with_same_version_replaces(self, client, sample_trade):
        """Test that same version trade is queued for async processing"""
        # Create initial trade
        response1 = client.post("/trades", json=sample_trade)
        assert response1.status_code == status.HTTP_202_ACCEPTED

        # Create trade with same version but different data
        same_version_trade = sample_trade.copy()
        same_version_trade["counter_party_id"] = "CP-UPDATED"
        
        response2 = client.post("/trades", json=same_version_trade)
        # Both get queued (202), consumer handles replacement logic
        assert response2.status_code == status.HTTP_202_ACCEPTED

        # Verify only one version exists
        response3 = client.get(f"/trades/{sample_trade['trade_id']}/{sample_trade['version']}")
        assert response3.status_code == status.HTTP_200_OK
        assert response3.json()["counter_party_id"] == "CP-UPDATED"


class TestGetTrades:
    """Test retrieving trades"""

    def test_get_all_trades_empty(self, client):
        """Test getting all trades when database is empty"""
        response = client.get("/trades")
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == []

    def test_get_all_trades(self, client, sample_trade):
        """Test getting all trades"""
        # Create multiple trades
        client.post("/trades", json=sample_trade)
        
        trade2 = sample_trade.copy()
        trade2["trade_id"] = "T2"
        client.post("/trades", json=trade2)

        response = client.get("/trades")
        assert response.status_code == status.HTTP_200_OK
        assert len(response.json()) == 2

    def test_get_trade_by_id_and_version(self, client, sample_trade):
        """Test getting specific trade by ID and version"""
        # Create trade
        client.post("/trades", json=sample_trade)

        # Get trade
        response = client.get(f"/trades/{sample_trade['trade_id']}/{sample_trade['version']}")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["trade_id"] == sample_trade["trade_id"]
        assert response.json()["version"] == sample_trade["version"]

    def test_get_nonexistent_trade_fails(self, client):
        """Test getting non-existent trade returns 404"""
        response = client.get("/trades/NONEXISTENT/1")
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_latest_trade_version(self, client, sample_trade):
        """Test getting latest version of a trade"""
        # Create version 1
        client.post("/trades", json=sample_trade)

        # Create version 2
        v2_trade = sample_trade.copy()
        v2_trade["version"] = 2
        v2_trade["counter_party_id"] = "CP-2"
        client.post("/trades", json=v2_trade)

        # Create version 3
        v3_trade = sample_trade.copy()
        v3_trade["version"] = 3
        v3_trade["counter_party_id"] = "CP-3"
        client.post("/trades", json=v3_trade)

        # Get latest version
        response = client.get(f"/trades/{sample_trade['trade_id']}/latest")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["version"] == 3
        assert response.json()["counter_party_id"] == "CP-3"


class TestUpdateTrade:
    """Test updating trades"""

    def test_update_trade_success(self, client, sample_trade):
        """Test trade update submission (async via Kafka)"""
        # Create trade
        client.post("/trades", json=sample_trade)

        # Update trade
        update_data = {"counter_party_id": "CP-UPDATED"}
        response = client.put(
            f"/trades/{sample_trade['trade_id']}/{sample_trade['version']}",
            json=update_data
        )
        # Update returns 202 Accepted (async processing via Kafka)
        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.json()["status"] == "accepted"

    def test_update_nonexistent_trade_fails(self, client):
        """Test updating non-existent trade (queued but will fail in consumer)"""
        update_data = {"counter_party_id": "CP-UPDATED"}
        response = client.put("/trades/NONEXISTENT/1", json=update_data)
        # API returns 202 (queued), but consumer will fail to find the trade
        assert response.status_code == status.HTTP_202_ACCEPTED

    def test_update_trade_with_past_maturity_date_fails(self, client, sample_trade):
        """Test updating trade with past maturity date fails at schema level"""
        # Create trade
        client.post("/trades", json=sample_trade)

        # Try to update with past maturity date
        # Schema validation still happens at API level, so this should fail with 422
        update_data = {
            "maturity_date": (date.today() - timedelta(days=1)).isoformat()
        }
        response = client.put(
            f"/trades/{sample_trade['trade_id']}/{sample_trade['version']}",
            json=update_data
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestDeleteTrade:
    """Test deleting trades"""

    def test_delete_trade_success(self, client, sample_trade):
        """Test trade deletion submission (async via Kafka)"""
        # Create trade
        client.post("/trades", json=sample_trade)

        # Delete trade
        response = client.delete(f"/trades/{sample_trade['trade_id']}/{sample_trade['version']}")
        # Delete returns 202 Accepted (async processing via Kafka)
        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.json()["status"] == "accepted"

    def test_delete_nonexistent_trade_fails(self, client):
        """Test deleting non-existent trade (queued but will fail in consumer)"""
        response = client.delete("/trades/NONEXISTENT/1")
        # API returns 202 (queued), but consumer will fail to find the trade
        assert response.status_code == status.HTTP_202_ACCEPTED


class TestTradeExpiry:
    """Test automatic trade expiry"""

    def test_manual_expire_trades(self, client):
        """Test manual expiry trigger"""
        # Create trade with future maturity
        future_trade = {
            "trade_id": "T1",
            "version": 1,
            "counter_party_id": "CP-1",
            "book_id": "B1",
            "maturity_date": (date.today() + timedelta(days=30)).isoformat(),
            "created_date": date.today().isoformat(),
            "expired": False
        }
        client.post("/trades", json=future_trade)

        # Trigger expiry check
        response = client.post("/trades/expire")
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["count"] == 0  # No trades expired

        # Verify trade is still not expired
        trade_response = client.get(f"/trades/T1/1")
        assert trade_response.json()["expired"] == False


class TestPaginationAndLimits:
    """Test pagination"""

    def test_pagination(self, client, sample_trade):
        """Test pagination of trades"""
        # Create 5 trades
        for i in range(5):
            trade = sample_trade.copy()
            trade["trade_id"] = f"T{i+1}"
            client.post("/trades", json=trade)

        # Get first 2 trades
        response = client.get("/trades?skip=0&limit=2")
        assert response.status_code == status.HTTP_200_OK
        assert len(response.json()) == 2

        # Get next 2 trades
        response = client.get("/trades?skip=2&limit=2")
        assert response.status_code == status.HTTP_200_OK
        assert len(response.json()) == 2

        # Get all trades
        response = client.get("/trades?limit=10")
        assert response.status_code == status.HTTP_200_OK
        assert len(response.json()) == 5


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_invalid_version_zero(self, client, sample_trade):
        """Test that version 0 is rejected"""
        sample_trade["version"] = 0
        response = client.post("/trades", json=sample_trade)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_invalid_version_negative(self, client, sample_trade):
        """Test that negative version is rejected"""
        sample_trade["version"] = -1
        response = client.post("/trades", json=sample_trade)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_missing_required_fields(self, client):
        """Test that missing required fields are rejected"""
        incomplete_trade = {
            "trade_id": "T1",
            "version": 1
        }
        response = client.post("/trades", json=incomplete_trade)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_maturity_date_equals_today(self, client, sample_trade):
        """Test that maturity date equal to today is accepted"""
        sample_trade["maturity_date"] = date.today().isoformat()
        response = client.post("/trades", json=sample_trade)
        assert response.status_code == status.HTTP_202_ACCEPTED
