import unittest
from unittest.mock import MagicMock, patch

from src.map.relation_project_user import upsert_relation_project_user


class TestUpsertRelationProjectUser(unittest.TestCase):
    def setUp(self):
        self.patcher_database = patch("src.map.relation_project_user.Database")
        self.mock_database = self.patcher_database.start()
        self.mock_db_instance = MagicMock()
        self.mock_database.return_value = self.mock_db_instance

        self.patcher_logger = patch("src.map.relation_project_user.Logger")
        self.mock_logger = self.patcher_logger.start()

        self.patcher_get_all_projects = patch(
            "src.map.relation_project_user.get_all_projects"
        )
        self.mock_get_all_projects = self.patcher_get_all_projects.start()

        self.patcher_get_user_by_id = patch(
            "src.map.relation_project_user.get_user_by_id"
        )
        self.mock_get_user_by_id = self.patcher_get_user_by_id.start()

    def tearDown(self):
        patch.stopall()

    def test_upsert_relation_project_user_success(self):
        # Mock data
        self.mock_get_all_projects.return_value = [
            {
                "id": 1,
                "name": "Project 1",
                "owner": {"id": 101, "email": "user1@example.com"},
            },
            {
                "id": 2,
                "name": "Project 2",
                "owner": {"id": 102, "email": "user2@example.com"},
            },
        ]
        self.mock_get_user_by_id.side_effect = [
            {"id": 101, "email": "user1@example.com"},
            {"id": 102, "email": "user2@example.com"},
        ]

        mock_conn = self.mock_db_instance.get_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Mock database queries
        mock_cursor.fetchone.side_effect = [
            (1,),  # Internal project ID for Project 1
            (101,),  # Internal user ID for user1@example.com
            (2,),  # Internal project ID for Project 2
            (102,),  # Internal user ID for user2@example.com
        ]

        # Call the function
        upsert_relation_project_user()

        # Assertions
        self.assertGreater(self.mock_logger.info.call_count, 0)
        # self.assertEqual(mock_cursor.execute.call_count, 4)  # 2 projects * 2 queries each
        self.assertEqual(mock_conn.commit.call_count, 2)  # 2 inserts

    def test_upsert_relation_project_user_no_projects(self):
        # Mock no projects
        self.mock_get_all_projects.return_value = []

        # Call the function
        upsert_relation_project_user()

        # Assertions
        self.mock_logger.info.assert_any_call("Retrieved 0 projects from the taiga.")
        self.mock_db_instance.get_connection.assert_called_once()
        self.mock_db_instance.get_connection.return_value.commit.assert_not_called()

    def test_upsert_relation_project_user_project_not_found(self):
        # Mock data
        self.mock_get_all_projects.return_value = [
            {
                "id": 1,
                "name": "Project 1",
                "owner": {"id": 101, "email": "user1@example.com"},
            }
        ]
        self.mock_get_user_by_id.return_value = {
            "id": 101,
            "email": "user1@example.com",
        }

        mock_conn = self.mock_db_instance.get_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Mock database queries
        mock_cursor.fetchone.side_effect = [
            None,  # Project not found
        ]

        # Call the function
        upsert_relation_project_user()

        # Assertions
        self.mock_logger.warning.assert_any_call(
            "Project 'Project 1' not found in the database. Skipping."
        )
        mock_conn.commit.assert_not_called()

    def test_upsert_relation_project_user_user_not_found(self):
        # Mock data
        self.mock_get_all_projects.return_value = [
            {
                "id": 1,
                "name": "Project 1",
                "owner": {"id": 101, "email": "user1@example.com"},
            }
        ]
        self.mock_get_user_by_id.return_value = None  # User not found in Taiga

        mock_conn = self.mock_db_instance.get_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Call the function
        upsert_relation_project_user()

        # Assertions
        self.mock_logger.error.assert_any_call(
            "User 101 not found in Taiga... skipping project 1"
        )
        mock_conn.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
