import requests

from src.config import config
from src.utils.logger import Logger


def get_all_users_by_project(project_id):
    """Returns a list of users by project ID from Taiga."""
    try:
        client = config.TaigaClient()
        users = client.api.users.list(project=project_id)

        all_users = []

        for user in users:
            user_complete = client.api.users.get(resource_id=user.id)

            email = get_useremail_by_project_by_memberships(project_id, user.id)
            if email:
                user_complete.email = email
            else:
                Logger.error(
                    f"Email not found for user {user.id} in project {project_id}."
                )

            all_users.append(user_complete)

        return [vars(user) for user in all_users]
    except Exception as e:
        Logger.error(f"An error occurred while fetching users: {e}")
        return []


def get_user_by_id(user_id, project_id):
    """Returns a user by ID."""
    try:
        client = config.TaigaClient()
        user = client.api.users.get(resource_id=user_id)
        email = get_useremail_by_project_by_memberships(project_id, user_id)
        if email:
            user.email = email
        else:
            Logger.error(f"Email not found for user {user_id} in project {project_id}.")
        return vars(user)
    except Exception as e:
        Logger.error(f"An error occurred while fetching user {user_id}: {e}")
        return None


# In-memory cache for memberships
membership_cache = {}


def get_useremail_by_project_by_memberships(project_id, user_id):
    """Fetches the email of a user by user ID from project memberships in Taiga."""

    # Check if the email is already in the cache
    if user_id in membership_cache:
        return membership_cache[user_id]

    token = config.TaigaClient().api.token
    if not token:
        Logger.error(
            "No API token provided. Please set the TAIGA_API_TOKEN environment variable."
        )
        return None

    url = f"https://api.taiga.io/api/v1/memberships?project={project_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        memberships = response.json()

        for membership in memberships:
            user_id_from_membership = membership.get("user")
            user_email = membership.get("user_email")
            if user_id_from_membership and user_email:
                # Populate the cache with the user email
                membership_cache[user_id_from_membership] = user_email

            if user_id_from_membership == user_id:
                return user_email

        Logger.error(f"No user with ID {user_id} found in project {project_id}.")
        return None
    except requests.exceptions.RequestException as e:
        Logger.error(f"An error occurred while fetching project memberships: {e}")
        return None
