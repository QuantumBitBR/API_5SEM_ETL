from src.config import config
from src.utils.logger import Logger


def get_all_projects():
    """Returns a list of projects from Taiga."""
    try:
        client = config.TaigaClient()
        projects = client.api.projects.list()
        return [vars(project) for project in projects]
    except Exception as e:
        Logger.error(f"An error occurred while fetching projects: {e}")
        return []
