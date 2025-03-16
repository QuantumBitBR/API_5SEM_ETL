import unittest
from unittest.mock import MagicMock, patch

from src.extract.user_storys import get_user_storys_by_project


class TestGetUserStorysByProject(unittest.TestCase):

    @patch("src.extract.user_storys.config.TaigaClient")
    def test_get_user_storys_by_project_success(self, MockTaigaClient):
        # Arrange
        mock_client = MockTaigaClient.return_value
        mock_user_story = MagicMock()
        mock_user_storys = [mock_user_story]
        mock_client.api.user_stories.list.return_value = mock_user_storys

        # Act
        result = get_user_storys_by_project(1)

        # Assert
        self.assertEqual(result, [vars(mock_user_story)])
        mock_client.api.user_stories.list.assert_called_once_with(project=1)

    @patch("src.extract.user_storys.config.TaigaClient")
    @patch("src.extract.user_storys.Logger")
    def test_get_user_storys_by_project_exception(self, MockLogger, MockTaigaClient):
        # Arrange
        mock_client = MockTaigaClient.return_value
        mock_client.api.user_stories.list.side_effect = Exception("API error")

        # Act
        result = get_user_storys_by_project(1)

        # Assert
        self.assertEqual(result, [])
        MockLogger.error.assert_called_once_with(
            "An error occurred while fetching user stories for project 1: API error"
        )


if __name__ == "__main__":
    unittest.main()
