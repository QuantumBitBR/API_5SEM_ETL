import datetime

from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.extract.users import get_user_by_id
from src.utils.logger import Logger


def process_data_2_fact_eficiencia():

    select_internal_project_id = (
        "SELECT id FROM public.dim_projeto WHERE LOWER(nome) = LOWER(%s)"
    )
    select_internal_user_id = (
        "SELECT id FROM public.dim_usuario WHERE LOWER(email) = LOWER(%s)"
    )
    select_internal_user_story_id = (
        "SELECT id FROM public.dim_user_story WHERE id_taiga = CAST(%s AS BIGINT)"
    )

    select_eficiencia = "SELECT * FROM public.fato_eficiencia_user_story WHERE id_usuario = %s AND id_user_story = %s AND id_projeto = %s"

    insert_eficiencia = "INSERT INTO public.fato_eficiencia_user_story (id_usuario, id_user_story, id_projeto, tempo_medio) VALUES (%s, %s, %s, %s)"

    update_eficiencia = "UPDATE public.fato_eficiencia_user_story SET tempo_medio = %s WHERE id_usuario = %s AND id_user_story = %s AND id_projeto = %s"

    db = Database()

    projects = get_all_projects()

    Logger.info(f"Retrieved {len(projects)} projects from source")

    for project in projects:

        conn = db.get_connection()
        try:
            cursor = conn.cursor()
            Logger.info(f"Processing project: {project['name']} (ID: {project['id']})")

            cursor.execute(select_internal_project_id, (project["name"],))
            internal_project_id = cursor.fetchone()[0]
            Logger.info(
                f"Fetched internal project ID: {internal_project_id} for project {project['name']}"
            )

            stories = get_user_storys_by_project(project["id"])
            Logger.info(
                f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
            )

            for story in stories:
                Logger.info(
                    f"Processing story: {story['id']} from project {project['name']}"
                )

                internal_user_id = None
                internal_user_story_id = None

                created_date = datetime.datetime.strptime(
                    story["created_date"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                finish_date = story["finish_date"]
                if finish_date is None:
                    Logger.warning(
                        f"Finish date is None for story {story['id']}... skipping"
                    )
                    continue
                else:
                    finish_date = datetime.datetime.strptime(
                        finish_date, "%Y-%m-%dT%H:%M:%S.%fZ"
                    )

                duration = (
                    finish_date - created_date
                ).total_seconds() / 60  # Convert duration to minutes
                Logger.info(
                    f"Story {story['id']} - Created Date: {created_date}, Finish Date: {finish_date}, Duration (minutes): {duration}"
                )

                assigned_to_extra_info = story.get("assigned_to_extra_info")
                taiga_user_id = (
                    assigned_to_extra_info.get("id") if assigned_to_extra_info else None
                )
                if taiga_user_id is None:
                    Logger.warning(
                        f"Assigned user is None for story {story['id']}... skipping"
                    )
                    continue

                Logger.info(f"Fetching user details for Taiga ID: {taiga_user_id}")
                user_complete = get_user_by_id(taiga_user_id)
                if user_complete is None:
                    Logger.error(
                        f"User {taiga_user_id} not found in Taiga... skipping story {story['id']}"
                    )
                    continue

                Logger.info(f"Fetched user details: {user_complete}")
                cursor.execute(select_internal_user_id, (user_complete["email"],))
                internal_user_id = cursor.fetchone()[0]
                Logger.info(
                    f"Fetched internal user ID: {internal_user_id} for user {user_complete['email']}"
                )

                cursor.execute(select_internal_user_story_id, (story["id"],))
                internal_user_story_id = cursor.fetchone()[0]
                Logger.info(
                    f"Fetched internal user story ID: {internal_user_story_id} for user story {story['id']}"
                )

                cursor.execute(
                    select_eficiencia,
                    (internal_user_id, internal_user_story_id, internal_project_id),
                )
                existing_record = cursor.fetchone()

                if existing_record:
                    Logger.info(
                        f"Existing record found for user {internal_user_id}, story {internal_user_story_id}, project {internal_project_id}"
                    )
                    cursor.execute(
                        update_eficiencia,
                        (
                            duration,
                            internal_user_id,
                            internal_user_story_id,
                            internal_project_id,
                        ),
                    )
                    Logger.info(
                        f"Updated efficiency for user {internal_user_id}, story {internal_user_story_id}, project {internal_project_id}"
                    )
                else:
                    Logger.info(
                        f"No existing record found for user {internal_user_id}, story {internal_user_story_id}, project {internal_project_id}"
                    )
                    cursor.execute(
                        insert_eficiencia,
                        (
                            internal_user_id,
                            internal_user_story_id,
                            internal_project_id,
                            duration,
                        ),
                    )
                    Logger.info(
                        f"Inserted efficiency for user {internal_user_id}, story {internal_user_story_id}, project {internal_project_id}"
                    )

        except Exception as e:
            Logger.error(
                f"An error occurred while processing project {project['name']} (ID: {project['id']}): {e}"
            )
            conn.rollback()
        finally:
            conn.commit()
            conn.close()
            Logger.info(
                f"Finished processing project: {project['name']} (ID: {project['id']})"
            )
