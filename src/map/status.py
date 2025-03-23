from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.utils.logger import Logger


def upsert_all_status():
    Logger.info("Starting upsert_all_status process")

    db = Database()
    projects = get_all_projects()
    Logger.info(f"Retrieved {len(projects)} projects from the database.")

    select_status_query = (
        "SELECT * FROM public.dim_status WHERE LOWER(tipo) = LOWER(%s)"
    )
    insert_status_query = "INSERT INTO public.dim_status (tipo) VALUES (LOWER(%s))"

    conn = db.get_connection()
    try:
        for project in projects:
            stories = get_user_storys_by_project(project["id"])
            Logger.info(
                f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
            )
            for story in stories:
                staus_in_user = story["status_extra_info"]

                try:
                    cursor = conn.cursor()
                    cursor.execute(select_status_query, (staus_in_user["name"],))
                    status_in_bd = cursor.fetchone()

                    if status_in_bd is None:
                        cursor.execute(insert_status_query, (staus_in_user["name"],))
                        conn.commit()
                        Logger.info(f"Inserted status {staus_in_user['name']}")
                    else:
                        Logger.info(
                            f"Status {staus_in_user['name']} already exists in the database"
                        )
                except Exception as e:
                    Logger.error(
                        f"Error processing status {staus_in_user['name']}: {e}"
                    )
                finally:
                    cursor.close()

    except Exception as e:
        Logger.error(f"Error processing projects: {e}")
    finally:
        db.release_connection(conn)

    Logger.info(f"Processed all projects. Total projects: {len(projects)}")
    return len(projects)
