from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.utils.logger import Logger


class FactProgressUserStorie:
    project_id: int
    tag_id: int
    count_user_story: int


def process_fact_tag_user_storie():

    Logger.info("Starting upsert_fact_progress_user_storie process")
    extract = extract_data_2_fact_tag_user_storie()

    Logger.info("Zeroing all progress quantities...")
    zero_all_progress()

    Logger.info(f"Upserting fact progress user storie...")
    upsert_fact_progress_user_storie(extract)


def extract_data_2_fact_tag_user_storie():

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
        try:
            for story in stories:
                internal_id_project = None
                internal_id_tag = []

                cursor = conn.cursor()
                cursor.execute(select_id_project, (project["name"],))
                internal_id_project = cursor.fetchone()[0]
                Logger.info(
                    f"Fetched internal project ID: {internal_id_project} for project {project['name']}"
                )

                for tag in story["tags"]:
                    cursor.execute(select_id_tag, (tag[0],))
                    tag_id = cursor.fetchone()[0]
                    internal_id_tag.append(tag_id)
                    Logger.info(f"Fetched internal tag ID: {tag_id} for tag {tag[0]}")

               
                for tag in internal_id_tag:
                    internal_key = f"{internal_id_project}-{tag}"
                    if internal_key in etl_progress:
                        etl_progress[internal_key].count_user_story += 1
                        Logger.info(f"Incremented count for key: {internal_key}")
                    else:
                        etl_progress[internal_key] = FactProgressUserStorie()
                        etl_progress[internal_key].project_id = internal_id_project
                        etl_progress[internal_key].tag_id = tag
                        etl_progress[internal_key].count_user_story = 1
                        Logger.info(f"Created new entry for key: {internal_key}")
        except Exception as e:
            Logger.error(f"An error occurred: {e}")
        finally:
            cursor.close()
            db.release_connection(conn)
            Logger.info("Database connection closed for project processing")

    return etl_progress


def upsert_fact_progress_user_storie(etl_progress):

    select_fact_progress = "SELECT * FROM public.fato_tag_user_story WHERE id_projeto = %s AND id_tag = %s"
    insert_fact_progress = "INSERT INTO public.fato_tag_user_story (id_projeto, id_tag, quantidade_user_story) VALUES (%s, %s, %s)"
    update_fact_progress = "UPDATE public.fato_tag_user_story SET quantidade_user_story = %s WHERE id_projeto = %s AND id_tag = %s"

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
                    etl_progress[key].tag_id,
                ),
            )
            result = cursor.fetchone()
            if not result:
                Logger.info(f"Inserting new fact progress for key: {key}")
                cursor.execute(
                    insert_fact_progress,
                    (
                        etl_progress[key].project_id,
                        etl_progress[key].tag_id,
                        etl_progress[key].count_user_story,
                    ),
                )
                conn.commit()
                Logger.info(f"Inserted fact progress for key: {key}")
            else:
                Logger.info(f"Fact progress already exists for key: {key}. Updating...")
                cursor.execute(
                    update_fact_progress,
                    (
                        etl_progress[key].count_user_story,
                        etl_progress[key].project_id,
                        etl_progress[key].tag_id,
                    ),
                )
                conn.commit()
                Logger.info(f"Updated fact progress for key: {key}")
    except Exception as e:
        Logger.error(f"An error occurred: {e}")
    finally:
        cursor.close()
        db.release_connection(conn)
        Logger.info("Database connection closed")


def zero_all_progress():
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE public.fato_tag_user_story SET quantidade_user_story = 0"
        )
        conn.commit()
        Logger.info("All progress quantities updated to 0")
    except Exception as e:
        Logger.error(f"An error occurred while updating progress quantities: {e}")
    finally:
        cursor.close()
        db.release_connection(conn)
        Logger.info("Database connection closed")
