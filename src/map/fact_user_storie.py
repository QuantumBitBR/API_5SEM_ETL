from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.utils.logger import Logger


def upsert_fact_user_storie():
    Logger.info("Starting upsert_fact_user_storie process")
    db = Database()
    conn = db.get_connection()

    tags_in_bd = get_tags_from_db(conn)
    projects = get_all_projects()
    Logger.info(f"Retrieved {len(projects)} projects from Taiga.")

    tags_by_project = process_projects(projects, tags_in_bd, conn)

    upsert_tags(tags_by_project, conn)

    db.release_connection(conn)


def get_tags_from_db(conn):
    select_tag_query = "SELECT * FROM public.dim_tag"
    tags_in_bd = {}
    try:
        cursor = conn.cursor()
        cursor.execute(select_tag_query)
        result = cursor.fetchall()
        for tag in result:
            tags_in_bd[tag[0]] = {"id": tag[0], "nome": tag[1]}
    except Exception as e:
        Logger.error(f"Error processing tags: {e}")
    finally:
        cursor.close()
    return tags_in_bd


def process_projects(projects, tags_in_bd, conn):
    tags_by_project = {}
    select_project = "SELECT id FROM public.dim_projeto WHERE name = LOWER(%s)"

    for project in projects:
        stories = get_user_storys_by_project(project["id"])
        Logger.info(
            f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
        )
        for story in stories:
            for tag in story["tags"]:
                if tag[0] in [t["nome"] for t in tags_in_bd.values()]:
                    cursor = conn.cursor()
                    cursor.execute(select_project, (project["name"],))
                    result = cursor.fetchone()
                    cursor.close()
                    if not result:
                        Logger.error(
                            f"Project '{project['name']}' not found in database."
                        )
                        continue

                    tag_id = next(
                        t["id"] for t in tags_in_bd.values() if t["nome"] == tag[0]
                    )
                    project_id = result[0]

                    if tag[0] in tags_by_project:
                        tags_by_project[tag[0]]["quantidade_user_stories"] += 1
                    else:
                        tags_by_project[tag[0]] = {
                            "id_tag": tag_id,
                            "nome": tag[0],
                            "id_projeto": project_id,
                            "quantidade_user_stories": 1,
                        }
                else:
                    Logger.error(f"Tag '{tag[0]}' not found in database.")
    return tags_by_project


def upsert_tags(tags_by_project, conn):
    insert_tag_query = "INSERT INTO public.fato_progresso_user_stories (id_tag, id_projeto, quantidade_user_stories) VALUES (%s, %s, %s)"
    update_tag_query = "UPDATE public.fato_progresso_user_stories SET quantidade_user_stories = %s WHERE id = %s"
    select_tag_query = "SELECT * FROM public.fato_progresso_user_stories WHERE id_tag = %s AND id_projeto = %s"

    project_ids = {tag["id_projeto"] for tag in tags_by_project.values()}
    for project_id in project_ids:
        set_all_count_to_zero(conn, project_id)

    try:
        for tag in tags_by_project.values():
            cursor = conn.cursor()
            try:
                cursor.execute(select_tag_query, (tag["id_tag"], tag["id_projeto"]))
                result = cursor.fetchone()
                if not result:
                    Logger.info(
                        f"Fact tag_id '{tag['id_tag']}' and projetc_id '{tag["id_projeto"]}' not found in database. Inserting new tag."
                    )
                    cursor.execute(
                        insert_tag_query,
                        (
                            tag["id_tag"],
                            tag["id_projeto"],
                            tag["quantidade_user_stories"],
                        ),
                    )
                    conn.commit()
                else:
                    Logger.info(
                        f"Fact tag_id '{tag['id_tag']}' and projetc_id '{tag["id_projeto"]}' already exists in the database. Updating tag."
                    )
                    cursor.execute(
                        update_tag_query, (tag["quantidade_user_stories"], result[0])
                    )
                    conn.commit()
            except Exception as e:
                Logger.error(f"Error processing tag '{tag['id_tag']}': {e}")
                conn.rollback()
            finally:
                cursor.close()
    except Exception as e:
        Logger.error(f"Error processing tags: {e}")


def set_all_count_to_zero(conn, id_projeto):
    Logger.info(f"Setting all user stories count to zero for project {id_projeto}")
    update_query = "UPDATE public.fato_progresso_user_stories SET quantidade_user_stories = 0 WHERE id_projeto = %s"
    try:
        cursor = conn.cursor()
        cursor.execute(update_query, (id_projeto,))
        conn.commit()
    except Exception as e:
        Logger.error(f"Error processing tags: {e}")
        conn.rollback()
    finally:
        cursor.close()
