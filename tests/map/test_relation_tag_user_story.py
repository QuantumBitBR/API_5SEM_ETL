import unittest
from unittest.mock import MagicMock, patch

from src.map.relation_tag_user_story import upsert_relation_tag_us


class TestUpsertRelationTagUS(unittest.TestCase):
    def setUp(self):
        self.patcher_database = patch("src.map.relation_tag_user_story.Database")
        self.mock_database = self.patcher_database.start()
        self.mock_db_instance = MagicMock()
        self.mock_database.return_value = self.mock_db_instance

        self.patcher_logger = patch("src.map.relation_tag_user_story.Logger")
        self.mock_logger = self.patcher_logger.start()

        self.patcher_get_all_projects = patch(
            "src.map.relation_tag_user_story.get_all_projects"
        )
        self.mock_get_all_projects = self.patcher_get_all_projects.start()

        self.patcher_get_user_storys_by_project = patch(
            "src.map.relation_tag_user_story.get_user_storys_by_project"
        )
        self.mock_get_user_storys_by_project = (
            self.patcher_get_user_storys_by_project.start()
        )

    def tearDown(self):
        patch.stopall()

    def test_upsert_relation_tag_us_success(self):
        # Mock data
        self.mock_get_all_projects.return_value = [
            {"id": 1, "name": "Project 1"},
            {"id": 2, "name": "Project 2"},
        ]
        self.mock_get_user_storys_by_project.side_effect = [
            [{"id": 101, "tags": [("tag1",), ("tag2",)]}],
            [{"id": 201, "tags": [("tag3",)]}],
        ]

        mock_conn = self.mock_db_instance.get_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Mock database queries
        mock_cursor.fetchone.side_effect = [
            (1,),  # Tag ID for tag1
            (101,),  # User story ID for story 101
            None,  # No existing relation for tag1 and story 101
            (2,),  # Tag ID for tag2
            (101,),  # User story ID for story 101
            None,  # No existing relation for tag2 and story 101
            (3,),  # Tag ID for tag3
            (201,),  # User story ID for story 201
            None,  # No existing relation for tag3 and story 201
        ]

        # Call the function
        upsert_relation_tag_us()

        # Assertions
        self.assertGreater(self.mock_logger.info.call_count, 0)
        # self.assertEqual(mock_cursor.execute.call_count, 9)  # 3 tags * 3 queries each
        self.assertEqual(mock_conn.commit.call_count, 3)  # 3 inserts

    def test_upsert_relation_tag_us_no_projects(self):
        # Mock no projects
        self.mock_get_all_projects.return_value = []

        # Call the function
        upsert_relation_tag_us()

        # Assertions
        self.mock_logger.info.assert_any_call("Retrieved 0 projects from the taiga.")
        self.mock_db_instance.get_connection.assert_called_once()
        self.mock_db_instance.get_connection.return_value.commit.assert_not_called()

    def test_upsert_relation_tag_us_tag_not_found(self):
        # Mock data
        self.mock_get_all_projects.return_value = [{"id": 1, "name": "Project 1"}]
        self.mock_get_user_storys_by_project.return_value = [
            {"id": 101, "tags": [("tag1",)]}
        ]

        mock_conn = self.mock_db_instance.get_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Mock database queries
        mock_cursor.fetchone.side_effect = [
            None,  # Tag not found
        ]

        # Call the function
        upsert_relation_tag_us()

        # Assertions
        self.mock_logger.warning.assert_any_call(
            "Tag 'tag1' not found in the database. Skipping."
        )
        mock_conn.commit.assert_not_called()

    def test_upsert_relation_tag_us_user_story_not_found(self):
        # Mock data
        self.mock_get_all_projects.return_value = [{"id": 1, "name": "Project 1"}]
        self.mock_get_user_storys_by_project.return_value = [
            {"id": 101, "tags": [("tag1",)]}
        ]

        mock_conn = self.mock_db_instance.get_connection.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Mock database queries
        mock_cursor.fetchone.side_effect = [
            (1,),  # Tag ID for tag1
            None,  # User story not found
        ]

        # Call the function
        upsert_relation_tag_us()

        # Assertions
        self.mock_logger.warning.assert_any_call(
            "User story '101' not found in the database. Skipping."
        )
        mock_conn.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
