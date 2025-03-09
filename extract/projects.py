from config import config


def get_projects():
    """Returns a list of projects from Taiga."""

    client = config.TaigaClient()

    projects = client.api.projects.list()

    return [vars(project) for project in projects]
