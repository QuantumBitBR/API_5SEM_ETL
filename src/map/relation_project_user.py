from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.extract.users import get_user_by_id
from src.utils.logger import Logger


def upsert_relation_project_user():
    
    Logger.info(f"Starting upsert_relation_project_user process")
    
    db = Database()
    projects = get_all_projects()
    Logger.info(f"Retrieved {len(projects)} projects from the taiga.")
    
    select_internal_user_id = (
        "SELECT id FROM public.dim_usuario WHERE LOWER(email) = LOWER(%s)"
    )
    
    select_internal_project_id = "SELECT id FROM public.dim_projeto WHERE LOWER(nome) = LOWER(%s)"
    
    insert_relation = "INSERT INTO public.relacionamento_projeto_usuario (id_usuario, id_projeto) VALUES (%s, %s) ON CONFLICT (id_usuario, id_projeto) DO NOTHING"

    
    conn = None
    try:
        conn = db.get_connection()
        Logger.info("Database connection established.")
        
        for project in projects:
            Logger.info(f"Processing project: {project['name']} (ID: {project['id']})")
            cursor = conn.cursor()
            try:
                Logger.debug(f"Executing query to find project ID: {select_internal_project_id} with parameter: {project['name']}")
                cursor.execute(select_internal_project_id, (project["name"],))
                internal_project_id = cursor.fetchone()
                if not internal_project_id:
                    Logger.warning(
                        f"Project '{project['name']}' not found in the database. Skipping."
                    )
                    continue

                Logger.debug(f"Executing query to find user ID: {select_internal_user_id} with parameter: {project['owner']['id']}") 
                assigned_to_extra_info = project.get("owner")
                taiga_user_id = assigned_to_extra_info.get("id") if assigned_to_extra_info else None
                if taiga_user_id is None:
                    Logger.warning(f"Assigned user is None for project {project['id']}... skipping")
                    continue
                
                user_complete = get_user_by_id(taiga_user_id)
                if user_complete is None:
                    Logger.error(f"User {taiga_user_id} not found in Taiga... skipping project {project['id']}")
                    continue

                cursor.execute(select_internal_user_id, (user_complete["email"],))
                internal_user_id = cursor.fetchone()
                
                if not internal_user_id:
                    Logger.warning(f"User '{user_complete['email']}' not found in the database. Skipping.")
                    continue

                Logger.debug(f"Inserting relationship with query: {insert_relation} and parameters: {internal_user_id[0]}, {internal_project_id[0]}")
                cursor.execute(insert_relation, (internal_user_id[0], internal_project_id[0]))
                conn.commit()
                Logger.info(
                    f"Inserted relationship between user '{user_complete['email']}' and project '{project['name']}'"
                )
            except Exception as e:
                Logger.error(f"Error processing project {project['id']}: {str(e)}")
                conn.rollback()
            finally:
                cursor.close()
    except Exception as e:
        Logger.error(f"Error establishing database connection or processing projects: {str(e)}")
    finally:
        if conn:
            db.release_connection(conn)
            conn.close()
            Logger.info("Database connection closed.")
