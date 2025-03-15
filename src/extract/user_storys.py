from src.config import config


def get_user_storys_by_project(id_project):
    """Returns a list of user stories filtered by project."""

    client = config.TaigaClient()

    user_stories = client.api.user_stories.list(project=id_project)

    return [vars(user_story) for user_story in user_stories]
