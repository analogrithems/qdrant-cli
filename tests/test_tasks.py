from re import X
import unittest
from unittest.mock import patch, Mock
from invoke import MockContext
from qdrant_cli.tasks import create_full_snapshot, download_full_snapshot
import os


class TestTasks(unittest.TestCase):
    @patch("qdrant_cli.tasks._fetch_snapshot")
    @patch("os.environ.get")
    def test_download_full_snapshot(self, mock_get, mock_fetch_snapshot):
        mock_get.return_value = "http://localhost:6333"
        c = MockContext(run={})
        download_full_snapshot(c, "snapshot1")
        mock_get.assert_called_once_with("QDRANT_SERVER", "http://localhost:6333")
        mock_fetch_snapshot.assert_called_once()

    @patch("qdrant_cli.tasks.qdrant_client.QdrantClient")
    @patch("os.environ.get")
    def test_create_full_snapshot(self, mock_get, mock_qdrant_client):
        mock_get.return_value = "http://localhost:6333"
        mock_client = Mock()
        mock_qdrant_client.return_value = mock_client
        mock_client.create_full_snapshot.return_value = "response"
        c = MockContext(run={})
        create_full_snapshot(c, wait=True, server="http://localhost:6333")
        mock_client.create_full_snapshot.assert_called_once_with(wait=True)


if __name__ == "__main__":
    unittest.main()
