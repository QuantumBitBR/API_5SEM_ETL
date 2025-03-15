import logging

from src.config import config
from src.utils.logger import Logger


def get_user_storys_by_project(id_project):
    """Returns a list of user stories filtered by project."""
    try:
        client = config.TaigaClient()
        user_stories = client.api.user_stories.list(project=id_project)
        return [vars(user_story) for user_story in user_stories]
    except Exception as e:
        Logger.error(
            f"An error occurred while fetching user stories for project {id_project}: {e}"
        )
        return []
