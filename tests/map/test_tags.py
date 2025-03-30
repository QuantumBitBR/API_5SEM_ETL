import unittest
from unittest.mock import MagicMock, patch

from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.map.tags import upsert_all_tags
from src.utils.logger import Logger


class TestUpsertAllTags(unittest.TestCase):

    @patch("src.map.tags.Database")
    @patch("src.map.tags.get_all_projects")
    @patch("src.map.tags.get_user_storys_by_project")
    @patch("src.map.tags.Logger")
    def test_upsert_all_tags(
        self,
        mock_logger,
        mock_get_user_storys_by_project,
        mock_get_all_projects,
        mock_database,
    ):
        # Setup mock return values
        mock_db_instance = MagicMock()
        mock_database.return_value = mock_db_instance
        mock_conn = MagicMock()
        mock_db_instance.get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_get_all_projects.return_value = [
            {"id": 1, "name": "Project 1"},
            {"id": 2, "name": "Project 2"},
        ]
        mock_get_user_storys_by_project.side_effect = [
            [{"id": 101, "tags": [("tag1",), ("tag2",)]}],
            [{"id": 102, "tags": [("tag3",), ("tag4",)]}],
        ]

        # Call the function
        result = upsert_all_tags()

        # Assertions
        self.assertEqual(result, 2)
        mock_logger.info.assert_any_call("Starting upsert_all_tags process")
        mock_logger.info.assert_any_call("Retrieved 2 projects from the database.")
        mock_logger.info.assert_any_call(
            "Extracted 1 user stories from project Project 1 (ID: 1)"
        )
        mock_logger.info.assert_any_call(
            "Extracted 1 user stories from project Project 2 (ID: 2)"
        )
        mock_logger.info.assert_any_call(
            "Processing tag 'tag1' from user story ID: 101"
        )
        mock_logger.info.assert_any_call(
            "Processing tag 'tag2' from user story ID: 101"
        )
        mock_logger.info.assert_any_call(
            "Processing tag 'tag3' from user story ID: 102"
        )
        mock_logger.info.assert_any_call(
            "Processing tag 'tag4' from user story ID: 102"
        )
        mock_logger.info.assert_any_call("Processed all projects. Total projects: 2")

        mock_cursor.execute.assert_any_call(
            "SELECT * FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)", ("tag1",)
        )
        mock_cursor.execute.assert_any_call(
            "SELECT * FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)", ("tag2",)
        )
        mock_cursor.execute.assert_any_call(
            "SELECT * FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)", ("tag3",)
        )
        mock_cursor.execute.assert_any_call(
            "SELECT * FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)", ("tag4",)
        )

        mock_db_instance.release_connection.assert_called_once_with(mock_conn)


if __name__ == "__main__":
    unittest.main()
