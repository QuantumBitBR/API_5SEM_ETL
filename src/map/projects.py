from src.config.database import Database
from src.extract.projects import get_all_projects
from src.utils.logger import Logger


# upsert_all_projects find all projects and insert them in the database (pk is name)
def upsert_all_projects():
    Logger.info("Starting upsert_all_projects process")

    db = Database()
    projects = get_all_projects()
    Logger.info(f"Retrieved {len(projects)} projects from source")

    insert_query = "INSERT INTO public.dim_projeto (name) VALUES (LOWER(%s))"
    select_query = "SELECT * FROM public.dim_projeto WHERE LOWER(name) = LOWER(%s)"

    for project in projects:
        Logger.info(f"Processing project: {project['name']}")
        try:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute(select_query, (project["name"],))
            result = cursor.fetchone()
            if not result:
                Logger.info(f"Inserting project: {project['name']}")
                cursor.execute(insert_query, (project["name"],))
                conn.commit()
            else:
                Logger.info(f"Project already exists: {project['name']}")
        except Exception as e:
            Logger.error(f"Error processing project {project['name']}: {e}")
        finally:
            cursor.close()
            db.release_connection(conn)

    Logger.info("Completed upsert_all_projects process")
    return len(projects)
