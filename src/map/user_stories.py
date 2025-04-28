from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.extract.users import get_user_by_id
from src.utils.logger import Logger


def upsert_all_user_stories():
    Logger.info("Starting upsert_all_user_stories process")

    db = Database()
    projects = get_all_projects()
    Logger.info(f"Retrieved {len(projects)} projects from the database.")

    select_user_story_by_taiga_id = (
        "SELECT * FROM public.dim_user_story WHERE id_taiga = CAST(%s AS BIGINT)"
    )
    
    select_status_id_internal = (
        "SELECT id FROM public.dim_status WHERE LOWER(tipo) = LOWER(%s)"
    )
    
    select_user_internal_id = (
        "SELECT id FROM public.dim_usuario WHERE LOWER(email) = LOWER(%s)"
    )
    
    
    select_project_id_internal = (
        "SELECT id FROM public.dim_projeto WHERE LOWER(nome) = LOWER(%s)"
    )

    insert_user_story = "INSERT INTO public.dim_user_story (id_taiga, assunto, criado_em, finalizado_em, bloqueado, encerrado, data_limite, id_status, id_usuario, id_projeto) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    update_user_story = "UPDATE public.dim_user_story SET assunto = %s, criado_em = %s, finalizado_em = %s, bloqueado = %s, encerrado = %s, data_limite = %s, id_status = %s, id_usuario = %s, id_projeto = %s WHERE id_taiga = %s"

    conn = db.get_connection()
    try:
        for project in projects:
            
            cursor = conn.cursor()
            cursor.execute(select_project_id_internal, (project["name"],))
            internal_project_id = cursor.fetchone()
            if not internal_project_id:
                Logger.warning(
                    f"Project '{project['name']}' not found in the database. Skipping."
                )
                continue

            Logger.debug(
                f"Executing query to find user ID: {select_project_id_internal} with parameter: {project['owner']['id']}"
            )
            
            stories = get_user_storys_by_project(project["id"])
            Logger.info(
                f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
            )
            for story in stories:

                try:
                    cursor = conn.cursor()
                    cursor.execute(select_user_story_by_taiga_id, (story["id"],))
                    story_in_bd = cursor.fetchone()
                    
                    cursor.execute(
                        select_status_id_internal, (story["status_extra_info"]["name"],)
                    )
                    status_id_internal = cursor.fetchone()
                    
                    
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
                    cursor.execute(select_user_internal_id, (user_complete["email"],))
                    internal_user_id = cursor.fetchone()[0]
                    
                    
                    

                    if story_in_bd is None:
                        cursor.execute(
                            insert_user_story,
                            (
                                story["id"],
                                story["subject"],
                                story["created_date"],
                                story["finish_date"],
                                story["is_blocked"],
                                story["is_closed"],
                                story["due_date"],
                                status_id_internal[0], 
                                internal_user_id,
                                internal_project_id
                            ),
                        )
                        conn.commit()
                        Logger.info(f"Inserted story {story['id']}")
                    else:
                        cursor.execute(
                            update_user_story,
                            (
                                story["subject"],
                                story["created_date"],
                                story["finish_date"],
                                story["is_blocked"],
                                story["is_closed"],
                                story["due_date"],
                                status_id_internal[0],
                                internal_user_id,
                                internal_project_id,
                                story["id"],
                            ),
                        )
                        conn.commit()
                        Logger.info(f"Updated story {story['id']}")
                except Exception as e:
                    Logger.error(f"Error processing status {story['id']}: {e}")
                finally:
                    cursor.close()

    except Exception as e:
        Logger.error(f"Error processing projects: {e}")
    finally:
        db.release_connection(conn)

    Logger.info(f"Processed all projects. Total projects: {len(projects)}")
    return len(projects)
