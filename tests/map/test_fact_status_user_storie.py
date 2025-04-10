import unittest
from unittest.mock import MagicMock, patch

from src.map.fact_status_user_story import (
    extract_data_2_fact_status_user_story, process_fact_status_user_story)
from src.utils.logger import Logger


class TestFactStatusUserStorie(unittest.TestCase):

    @patch("src.map.fact_status_user_story.extract_data_2_fact_status_user_story")
    @patch("src.map.fact_status_user_story.zero_all_status")
    @patch("src.map.fact_status_user_story.upsert_fact_status_user_story")
    @patch.object(Logger, "info")
    def test_process_fact_status_user_story(
        self, mock_logger_info, mock_upsert, mock_zero, mock_extract
    ):
        # Arrange
        mock_extract.return_value = {"some_key": "some_value"}

        # Act
        process_fact_status_user_story()

        # Assert
        mock_logger_info.assert_any_call(
            "Starting upsert_fact_progress_user_storie process"
        )
        mock_extract.assert_called_once()
        mock_logger_info.assert_any_call("Zeroing all progress quantities...")
        mock_zero.assert_called_once()
        mock_logger_info.assert_any_call("Upserting fact status user storie...")
        mock_upsert.assert_called_once_with({"some_key": "some_value"})

    @patch("src.map.fact_status_user_story.get_all_projects")
    @patch("src.map.fact_status_user_story.get_user_storys_by_project")
    @patch("src.map.fact_status_user_story.Database")
    @patch.object(Logger, "info")
    @patch.object(Logger, "error")
    def test_extract_data_2_fact_status_user_story_exception(
        self,
        mock_logger_error,
        mock_logger_info,
        mock_db,
        mock_get_user_storys,
        mock_get_all_projects,
    ):
        # Arrange
        mock_get_all_projects.return_value = [{"id": 1, "name": "Project 1"}]
        mock_get_user_storys.return_value = [
            {"status_extra_info": {"name": "Status 1"}}
        ]
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Database error")

        # Act
        result = extract_data_2_fact_status_user_story()

        # Assert
        assert result == {}

        mock_logger_info.assert_any_call(
            "Starting extract_data_2_fact_progress_user_storie process"
        )
        mock_logger_info.assert_any_call(
            "Extracted 1 user stories from project Project 1 (ID: 1)"
        )
        mock_logger_error.assert_any_call("An error occurred: Database error")
        mock_logger_info.assert_any_call(
            "Database connection closed for project processing"
        )

        mock_cursor.close.assert_called()
        mock_db.return_value.release_connection.assert_called_with(mock_conn)
