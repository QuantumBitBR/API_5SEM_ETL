from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.utils.logger import Logger


def upsert_all_user_stories():
    Logger.info("Starting upsert_all_user_stories process")

    db = Database()
    projects = get_all_projects()
    Logger.info(f"Retrieved {len(projects)} projects from the database.")

    select_user_story_by_taiga_id = (
        "SELECT * FROM public.dim_user_story WHERE id_taiga = CAST(%s AS BIGINT)"
    )

    insert_user_story = "INSERT INTO public.dim_user_story (id_taiga, assunto, criado_em, finalizado_em, bloqueado, encerrado, data_limite) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    update_user_story = "UPDATE public.dim_user_story SET assunto = %s, criado_em = %s, finalizado_em = %s, bloqueado = %s, encerrado = %s, data_limite = %s WHERE id_taiga = %s"

    conn = db.get_connection()
    try:
        for project in projects:
            stories = get_user_storys_by_project(project["id"])
            Logger.info(
                f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
            )
            for story in stories:

                try:
                    cursor = conn.cursor()
                    cursor.execute(select_user_story_by_taiga_id, (story["id"],))
                    story_in_bd = cursor.fetchone()

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
