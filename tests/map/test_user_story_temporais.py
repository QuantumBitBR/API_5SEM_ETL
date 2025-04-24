import unittest
from unittest.mock import MagicMock, patch

from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.extract.users import get_user_by_id
from src.map.fact_user_story_temporais import process_data_2_fact_temporais
from src.utils.logger import Logger


class TestProcessData2FactTemporais(unittest.TestCase):

    @patch("src.map.fact_user_story_temporais.Database")
    @patch("src.map.fact_user_story_temporais.get_all_projects")
    @patch("src.map.fact_user_story_temporais.Logger")
    def test_process_data_no_projects(self, mock_logger, mock_get_all_projects, mock_database):
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_database.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock no projects
        mock_get_all_projects.return_value = []

        # Call the function
        process_data_2_fact_temporais()

        # Assertions
        mock_get_all_projects.assert_called_once()
        mock_cursor.execute.assert_called_once_with(
            "TRUNCATE TABLE public.fato_user_story_temporais RESTART IDENTITY CASCADE"
        )
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_database.return_value.release_connection.assert_called_once_with(mock_conn)

    @patch("src.map.fact_user_story_temporais.Database")
    @patch("src.map.fact_user_story_temporais.get_all_projects")
    @patch("src.map.fact_user_story_temporais.Logger")
    def test_process_data_exception_handling(self, mock_logger, mock_get_all_projects, mock_database):
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_database.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock exception during project fetching
        mock_get_all_projects.side_effect = Exception("Test Exception")

        # Call the function
        process_data_2_fact_temporais()

        # Assertions
        mock_get_all_projects.assert_called_once()
        mock_logger.error.assert_called_once_with(
            "An error occurred while processing data: Test Exception"
        )
        mock_cursor.close.assert_called_once()
        mock_database.return_value.release_connection.assert_called_once_with(mock_conn)


if __name__ == "__main__":
    unittest.main()