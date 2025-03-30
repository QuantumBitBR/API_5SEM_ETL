from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.utils.logger import Logger


class FactStatusUserStorie:
    project_id: int
    status_id: int
    count_user_story: int


def process_fact_status_user_story():

    Logger.info("Starting upsert_fact_progress_user_storie process")
    extract = extract_data_2_fact_status_user_storie()

    Logger.info("Zeroing all progress quantities...")
    zero_all_status()

    Logger.info(f"Upserting fact status user storie...")
    upsert_fact_status_user_storie(extract)


def extract_data_2_fact_status_user_storie():

    select_id_project = (
        "SELECT id FROM public.dim_projeto WHERE LOWER(nome) = LOWER(%s)"
    )
    select_id_tag = "SELECT id FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)"
    select_id_status = "SELECT id FROM public.dim_status WHERE LOWER(tipo) = LOWER(%s)"

    Logger.info("Starting extract_data_2_fact_progress_user_storie process")
    allProjects = get_all_projects()

    etl_progress = {}

    for project in allProjects:
        stories = get_user_storys_by_project(project["id"])
        Logger.info(
            f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
        )

        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        try:
            for story in stories:
                internal_id_project = None
                internal_id_status = None

                cursor.execute(select_id_project, (project["name"],))
                internal_id_project = cursor.fetchone()[0]
                Logger.info(
                    f"Fetched internal project ID: {internal_id_project} for project {project['name']}"
                )

                cursor.execute(select_id_status, (story["status_extra_info"]["name"],))
                internal_id_status = cursor.fetchone()[0]
                Logger.info(
                    f"Fetched internal status ID: {internal_id_status} for status {story['status_extra_info']['name']}"
                )

                internal_key = f"{internal_id_project}-{internal_id_status}"
                if internal_key in etl_progress:
                    etl_progress[internal_key].count_user_story += 1
                    Logger.info(f"Incremented count for key: {internal_key}")
                else:
                    etl_progress[internal_key] = FactStatusUserStorie()
                    etl_progress[internal_key].project_id = internal_id_project
                    etl_progress[internal_key].status_id = internal_id_status
                    etl_progress[internal_key].count_user_story = 1
                    Logger.info(f"Created new entry for key: {internal_key}")

        except Exception as e:
            Logger.error(f"An error occurred: {e}")
        finally:
            cursor.close()
            db.release_connection(conn)
            Logger.info("Database connection closed for project processing")

    return etl_progress


def upsert_fact_status_user_storie(etl_progress):

    select_fact_progress = "SELECT * FROM public.fato_status_user_story WHERE id_projeto = %s AND id_status = %s"
    insert_fact_progress = "INSERT INTO public.fato_status_user_story (id_projeto, id_status, quantidade_user_story) VALUES (%s, %s, %s)"
    update_fact_progress = "UPDATE public.fato_status_user_story SET quantidade_user_story = %s WHERE id_projeto = %s AND id_status = %s"

    db = Database()
    conn = db.get_connection()

    try:
        cursor = conn.cursor()
        for key in etl_progress:
            Logger.info(f"Processing key: {key}")
            cursor.execute(
                select_fact_progress,
                (
                    etl_progress[key].project_id,
                    etl_progress[key].status_id,
                ),
            )
            result = cursor.fetchone()
            if not result:
                Logger.info(f"Inserting new fact status for key: {key}")
                cursor.execute(
                    insert_fact_progress,
                    (
                        etl_progress[key].project_id,
                        etl_progress[key].status_id,
                        etl_progress[key].count_user_story,
                    ),
                )
                conn.commit()
                Logger.info(f"Inserted fact status for key: {key}")
            else:
                Logger.info(f"Fact status already exists for key: {key}. Updating...")
                cursor.execute(
                    update_fact_progress,
                    (
                        etl_progress[key].count_user_story,
                        etl_progress[key].project_id,
                        etl_progress[key].status_id,
                    ),
                )
                conn.commit()
                Logger.info(f"Updated fact status for key: {key}")
    except Exception as e:
        Logger.error(f"An error occurred: {e}")
    finally:
        cursor.close()
        db.release_connection(conn)
        Logger.info("Database connection closed")


def zero_all_status():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE public.fato_status_user_story SET quantidade_user_story = 0"
        )
        conn.commit()
        Logger.info("All progress quantities updated to 0")
    except Exception as e:
        Logger.error(f"An error occurred while updating progress quantities: {e}")
    finally:
        cursor.close()
        db.release_connection(conn)
        Logger.info("Database connection closed")
