import unittest
from unittest.mock import MagicMock, patch

from src.map.fact_tag_user_story import (process_fact_tag_user_story,
                                         zero_all_progress)


class TestProcessFactTagUserStorie(unittest.TestCase):

    @patch("src.map.fact_tag_user_story.Logger")
    @patch("src.map.fact_tag_user_story.extract_data_2_fact_tag_user_story")
    @patch("src.map.fact_tag_user_story.zero_all_progress")
    @patch("src.map.fact_tag_user_story.upsert_fact_progress_user_storie")
    def test_process_fact_tag_user_story(
        self, mock_upsert, mock_zero, mock_extract, mock_logger
    ):
        # Arrange
        mock_extract.return_value = "mocked_extract_data"

        # Act
        process_fact_tag_user_story()

        # Assert
        mock_logger.info.assert_any_call(
            "Starting upsert_fact_progress_user_storie process"
        )
        mock_extract.assert_called_once()
        mock_logger.info.assert_any_call("Zeroing all progress quantities...")
        mock_zero.assert_called_once()
        mock_logger.info.assert_any_call("Upserting fact progress user storie...")
        mock_upsert.assert_called_once_with("mocked_extract_data")

    @patch("src.map.fact_tag_user_story.Logger")
    @patch("src.map.fact_tag_user_story.extract_data_2_fact_tag_user_story")
    @patch("src.map.fact_tag_user_story.zero_all_progress")
    @patch("src.map.fact_tag_user_story.upsert_fact_progress_user_storie")
    def test_process_fact_tag_user_story(
        self, mock_upsert, mock_zero, mock_extract, mock_logger
    ):
        # Arrange
        mock_extract.return_value = "mocked_extract_data"

        # Act
        process_fact_tag_user_story()

        # Assert
        mock_logger.info.assert_any_call(
            "Starting upsert_fact_progress_user_storie process"
        )
        mock_extract.assert_called_once()
        mock_logger.info.assert_any_call("Zeroing all progress quantities...")
        mock_zero.assert_called_once()
        mock_logger.info.assert_any_call("Upserting fact progress user storie...")
        mock_upsert.assert_called_once_with("mocked_extract_data")

    @patch("src.map.fact_tag_user_story.Database")
    @patch("src.map.fact_tag_user_story.Logger")
    def test_zero_all_progress(self, mock_logger, mock_database):
        # Arrange
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_database.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Act
        zero_all_progress()

        # Assert
        mock_cursor.execute.assert_called_once_with(
            "UPDATE public.fato_tag_user_story SET quantidade_user_story = 0"
        )
        mock_conn.commit.assert_called_once()
        mock_logger.info.assert_any_call("All progress quantities updated to 0")
        mock_cursor.close.assert_called_once()
        mock_database.return_value.release_connection.assert_called_once_with(mock_conn)
        mock_logger.info.assert_any_call("Database connection closed")

    @patch("src.map.fact_tag_user_story.Database")
    @patch("src.map.fact_tag_user_story.Logger")
    def test_zero_all_progress_exception(self, mock_logger, mock_database):
        # Arrange
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_database.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Test exception")

        # Act
        zero_all_progress()

        # Assert
        mock_logger.error.assert_called_once_with(
            "An error occurred while updating progress quantities: Test exception"
        )
        mock_cursor.close.assert_called_once()
        mock_database.return_value.release_connection.assert_called_once_with(mock_conn)
        mock_logger.info.assert_any_call("Database connection closed")


if __name__ == "__main__":
    unittest.main()
