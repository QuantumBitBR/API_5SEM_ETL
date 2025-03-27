from src.config import config
from src.utils.logger import Logger


def get_all_users():
    """Returns a list of projects from Taiga."""
    try:
        client = config.TaigaClient()
        users = client.api.users.list()
        
        all_users = []
        
        for user in users:
            user_complete = client.api.users.get(resource_id=user.id)
            all_users.append(user_complete)
        
        return [vars(user) for user in all_users]
    except Exception as e:
        Logger.error(f"An error occurred while fetching users: {e}")
        return []
