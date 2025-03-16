import unittest
from unittest.mock import MagicMock, patch

from src.extract.projects import get_all_projects
from src.utils.logger import Logger


class TestGetAllProjects(unittest.TestCase):

    @patch("src.extract.projects.config.TaigaClient")
    def test_get_all_projects_success(self, MockTaigaClient):
        # Mock the TaigaClient and its response
        mock_client = MockTaigaClient.return_value
        mock_project = MagicMock()
        mock_project_data = {"id": 1, "name": "Test Project"}
        mock_project.__dict__ = mock_project_data
        mock_client.api.projects.list.return_value = [mock_project]

        # Call the function
        result = get_all_projects()

        # Assertions
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Test Project")

    @patch("src.extract.projects.config.TaigaClient")
    @patch.object(Logger, "error")
    def test_get_all_projects_exception(self, mock_logger_error, MockTaigaClient):
        # Mock the TaigaClient to raise an exception
        MockTaigaClient.side_effect = Exception("Test Exception")

        # Call the function
        result = get_all_projects()

        # Assertions
        self.assertEqual(result, [])
        mock_logger_error.assert_called_once_with(
            "An error occurred while fetching projects: Test Exception"
        )


if __name__ == "__main__":
    unittest.main()
