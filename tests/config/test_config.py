import unittest
from unittest.mock import MagicMock, patch

from src.config.config import TaigaClient


class TestTaigaClient(unittest.TestCase):

    @patch("src.config.config.TaigaAPI")
    @patch("src.config.config.TAIGA_API_URL", "https://api.taiga.io")
    @patch("src.config.config.TAIGA_USERNAME", "test_user")
    @patch("src.config.config.TAIGA_PASSWORD", "test_password")
    def test_singleton_instance(self, mock_taiga_api):
        client1 = TaigaClient()
        client2 = TaigaClient()
        self.assertIs(client1, client2, "TaigaClient is not a singleton")

    @patch("src.config.config.TaigaAPI")
    @patch("src.config.config.TAIGA_API_URL", "https://api.taiga.io")
    @patch("src.config.config.TAIGA_USERNAME", "test_user")
    @patch("src.config.config.TAIGA_PASSWORD", "test_password")
    def test_authentication(self, mock_taiga_api):
        mock_api_instance = MagicMock()
        mock_taiga_api.return_value = mock_api_instance

        client = TaigaClient()

        mock_taiga_api.assert_called_once_with(
            host="https://api.taiga.io", tls_verify=False
        )
        mock_api_instance.auth.assert_called_once_with(
            username="test_user", password="test_password"
        )
        self.assertEqual(client.api, mock_api_instance)


if __name__ == "__main__":
    unittest.main()
