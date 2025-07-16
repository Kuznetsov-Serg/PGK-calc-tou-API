from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health_check():
    response = client.get("/health_check_old")
    assert response.status_code == 200
    assert response.json() == {"message": "OK"}
