import pytest
from datetime import datetime, timedelta
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


class TestUpdateTrade:
    """Test updating trades"""

    def test_update_trade_success(self, client, sample_trade):
        """Test trade update submission (async via Kafka)"""
        # Create trade
        client.post("/trades", json=sample_trade)

        # Update trade
        update_data = {"counter_party_id": "CP-UPDATED"}
        response = client.put(f"/trades/{sample_trade['trade_id']}/{sample_trade['version']}", json=update_data)
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
        update_data = {"maturity_date": (datetime.now() - timedelta(days=1)).isoformat()}
        response = client.put(f"/trades/{sample_trade['trade_id']}/{sample_trade['version']}", json=update_data)
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

    def test_missing_required_fields(self, client, incomplete_trade):
        """Test that missing required fields are rejected"""
        response = client.post("/trades", json=incomplete_trade)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_maturity_date_equals_today(self, client, sample_trade):
        """Test that maturity datetime later today is accepted"""
        sample_trade["maturity_date"] = (datetime.now() + timedelta(hours=1)).isoformat()
        response = client.post("/trades", json=sample_trade)
        assert response.status_code == status.HTTP_202_ACCEPTED

    def test_maturity_datetime_earlier_today_is_accepted(self, client, sample_trade):
        """Validation should reject past dates, not earlier times on the same date"""
        sample_trade["maturity_date"] = datetime.now().replace(hour=0, minute=1, second=0, microsecond=0).isoformat()
        response = client.post("/trades", json=sample_trade)
        assert response.status_code == status.HTTP_202_ACCEPTED
