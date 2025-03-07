
from config import config


def get_projects():
    api = config.authenticate()
    projects = api.projects.list()
    return [
        {
            "id": project.id,
            "name": project.name,
            "description": project.description,
            "created_date": project.created_date,
        }
        for project in projects
    ]