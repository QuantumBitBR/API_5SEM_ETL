import unittest
from unittest.mock import MagicMock, patch

from src.map.fact_progress_user_storie import (
    extract_data_2_fact_progress_user_storie,
    process_fact_progress_user_storie,
)


class TestFactProgressUserStorie(unittest.TestCase):

    @patch("src.map.fact_progress_user_storie.Logger")
    @patch("src.map.fact_progress_user_storie.extract_data_2_fact_progress_user_storie")
    @patch("src.map.fact_progress_user_storie.zero_all_progress")
    @patch("src.map.fact_progress_user_storie.upsert_fact_progress_user_storie")
    def test_process_fact_progress_user_storie(
        self, mock_upsert, mock_zero, mock_extract, mock_logger
    ):
        # Arrange
        mock_extract.return_value = "mocked_extract_data"

        # Act
        process_fact_progress_user_storie()

        # Assert
        mock_logger.info.assert_any_call(
            "Starting upsert_fact_progress_user_storie process"
        )
        mock_extract.assert_called_once()
        mock_logger.info.assert_any_call("Zeroing all progress quantities...")
        mock_zero.assert_called_once()
        mock_logger.info.assert_any_call("Upserting fact progress user storie...")
        mock_upsert.assert_called_once_with("mocked_extract_data")

    @patch("src.map.fact_progress_user_storie.get_all_projects")
    @patch("src.map.fact_progress_user_storie.get_user_storys_by_project")
    @patch("src.map.fact_progress_user_storie.Database")
    @patch("src.map.fact_progress_user_storie.Logger")
    def test_extract_data_2_fact_progress_user_storie(
        self,
        mock_logger,
        mock_database,
        mock_get_user_storys_by_project,
        mock_get_all_projects,
    ):
        # Arrange
        mock_get_all_projects.return_value = [
            {"id": 1, "name": "Project 1"},
            {"id": 2, "name": "Project 2"},
        ]
        mock_get_user_storys_by_project.side_effect = [
            [{"tags": [["tag1"]], "status_extra_info": {"name": "status1"}}],
            [{"tags": [["tag2"]], "status_extra_info": {"name": "status2"}}],
        ]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_database.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            [1],  # project id for Project 1
            [1],  # tag id for tag1
            [1],  # status id for status1
            [2],  # project id for Project 2
            [2],  # tag id for tag2
            [2],  # status id for status2
        ]

        # Act
        result = extract_data_2_fact_progress_user_storie()

        # Assert
        assert len(result) == 2
        assert result["1-1-1"].project_id == 1
        assert result["1-1-1"].tag_id == 1
        assert result["1-1-1"].status_id == 1
        assert result["1-1-1"].count_user_story == 1

        assert result["2-2-2"].project_id == 2
        assert result["2-2-2"].tag_id == 2
        assert result["2-2-2"].status_id == 2
        assert result["2-2-2"].count_user_story == 1

        mock_logger.info.assert_any_call(
            "Starting extract_data_2_fact_progress_user_storie process"
        )
        mock_logger.info.assert_any_call(
            "Extracted 1 user stories from project Project 1 (ID: 1)"
        )
        mock_logger.info.assert_any_call(
            "Extracted 1 user stories from project Project 2 (ID: 2)"
        )
        mock_logger.info.assert_any_call(
            "Fetched internal project ID: 1 for project Project 1"
        )
        mock_logger.info.assert_any_call("Fetched internal tag ID: 1 for tag tag1")
        mock_logger.info.assert_any_call(
            "Fetched internal status ID: 1 for status status1"
        )
        mock_logger.info.assert_any_call(
            "Fetched internal project ID: 2 for project Project 2"
        )
        mock_logger.info.assert_any_call("Fetched internal tag ID: 2 for tag tag2")
        mock_logger.info.assert_any_call(
            "Fetched internal status ID: 2 for status status2"
        )
        mock_logger.info.assert_any_call("Created new entry for key: 1-1-1")
        mock_logger.info.assert_any_call("Created new entry for key: 2-2-2")
        mock_logger.info.assert_any_call(
            "Database connection closed for project processing"
        )


if __name__ == "__main__":
    unittest.main()
