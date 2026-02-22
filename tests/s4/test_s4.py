import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from bdi_api.app import app

client = TestClient(app)


def test_download_returns_ok():
    """Test that the download endpoint returns OK"""
    with patch("bdi_api.s4.exercise.boto3.client") as mock_boto, \
         patch("bdi_api.s4.exercise.httpx.get") as mock_get:

        mock_response = MagicMock()
        mock_response.content = b"\x1f\x8b\x08test"
        mock_get.return_value = mock_response

        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        response = client.post("/api/s4/aircraft/download?file_limit=2")
        assert response.status_code == 200
        assert response.json() == "OK"


def test_download_uploads_correct_number_of_files():
    """Test that the download endpoint uploads the correct number of files to S3"""
    with patch("bdi_api.s4.exercise.boto3.client") as mock_boto, \
         patch("bdi_api.s4.exercise.httpx.get") as mock_get:

        mock_response = MagicMock()
        mock_response.content = b"\x1f\x8b\x08test"
        mock_get.return_value = mock_response

        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        client.post("/api/s4/aircraft/download?file_limit=5")
        assert mock_s3.put_object.call_count == 5


def test_download_stores_in_correct_s3_path():
    """Test that files are stored under raw/day=20231101/"""
    with patch("bdi_api.s4.exercise.boto3.client") as mock_boto, \
         patch("bdi_api.s4.exercise.httpx.get") as mock_get:

        mock_response = MagicMock()
        mock_response.content = b"\x1f\x8b\x08test"
        mock_get.return_value = mock_response

        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        client.post("/api/s4/aircraft/download?file_limit=1")

        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Key"].startswith("raw/day=20231101/")


def test_prepare_returns_ok():
    """Test that the prepare endpoint returns OK"""
    import gzip
    import json

    sample_data = json.dumps({"aircraft": [{"hex": "abc123"}]}).encode()
    compressed = gzip.compress(sample_data)

    with patch("bdi_api.s4.exercise.boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "raw/day=20231101/000000Z.json.gz"}]
        }

        mock_body = MagicMock()
        mock_body.read.return_value = compressed
        mock_s3.get_object.return_value = {"Body": mock_body}

        response = client.post("/api/s4/aircraft/prepare")
        assert response.status_code == 200
        assert response.json() == "OK"


def test_prepare_returns_ok_when_bucket_empty():
    """Test that prepare returns OK even if there are no files in S3"""
    with patch("bdi_api.s4.exercise.boto3.client") as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}

        response = client.post("/api/s4/aircraft/prepare")
        assert response.status_code == 200
        assert response.json() == "OK"
