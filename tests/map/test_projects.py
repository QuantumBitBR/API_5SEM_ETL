import unittest
from unittest.mock import MagicMock, patch

from src.config.database import Database
from src.extract.projects import get_all_projects
from src.map.projects import upsert_all_projects
from src.utils.logger import Logger


class TestUpsertAllProjects(unittest.TestCase):

    @patch("src.map.projects.Database")
    @patch("src.map.projects.get_all_projects")
    @patch("src.map.projects.Logger")
    def test_upsert_all_projects(
        self, mock_logger, mock_get_all_projects, mock_database
    ):
        # Mock data
        mock_projects = [{"name": "Project1"}, {"name": "Project2"}]
        mock_get_all_projects.return_value = mock_projects

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_database.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock cursor behavior
        mock_cursor.fetchone.side_effect = [
            None,
            {"name": "Project2"},
        ]  # First project not in DB, second project in DB

        # Call the function
        result = upsert_all_projects()

        # Assertions
        self.assertEqual(result, 2)
        mock_logger.info.assert_any_call("Starting upsert_all_projects process")
        mock_logger.info.assert_any_call("Retrieved 2 projects from source")
        mock_logger.info.assert_any_call("Processing project: Project1")
        mock_logger.info.assert_any_call("Inserting project: Project1")
        mock_logger.info.assert_any_call("Processing project: Project2")
        mock_logger.info.assert_any_call("Project already exists: Project2")
        mock_logger.info.assert_any_call("Completed upsert_all_projects process")

        # Ensure the correct SQL queries were executed
        mock_cursor.execute.assert_any_call(
            "SELECT * FROM public.dim_projeto WHERE LOWER(name) = LOWER(%s)",
            ("Project1",),
        )
        mock_cursor.execute.assert_any_call(
            "INSERT INTO public.dim_projeto (name) VALUES (LOWER(%s))", ("Project1",)
        )
        mock_cursor.execute.assert_any_call(
            "SELECT * FROM public.dim_projeto WHERE LOWER(name) = LOWER(%s)",
            ("Project2",),
        )

        # Ensure the connection and cursor were closed
        mock_cursor.close.assert_called()
        mock_database.return_value.release_connection.assert_called_with(mock_conn)

    @patch("src.map.projects.Database")
    @patch("src.map.projects.get_all_projects")
    @patch("src.map.projects.Logger")
    def test_upsert_all_projects_exception(
        self, mock_logger, mock_get_all_projects, mock_database
    ):
        # Mock data
        mock_projects = [{"name": "Project1"}]
        mock_get_all_projects.return_value = mock_projects

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_database.return_value.get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock cursor behavior to raise an exception
        mock_cursor.execute.side_effect = Exception("Database error")

        # Call the function
        result = upsert_all_projects()

        # Assertions
        self.assertEqual(result, 1)
        mock_logger.info.assert_any_call("Starting upsert_all_projects process")
        mock_logger.info.assert_any_call("Retrieved 1 projects from source")
        mock_logger.info.assert_any_call("Processing project: Project1")
        mock_logger.error.assert_any_call(
            "Error processing project Project1: Database error"
        )
        mock_logger.info.assert_any_call("Completed upsert_all_projects process")

        # Ensure the connection and cursor were closed
        mock_cursor.close.assert_called()
        mock_database.return_value.release_connection.assert_called_with(mock_conn)


if __name__ == "__main__":
    unittest.main()
