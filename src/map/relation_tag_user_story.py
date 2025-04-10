from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.utils.logger import Logger

# CREATE TABLE public.relacionamento_tag_user_story (
# 	id_tag int8 NOT NULL,
# 	id_user_story int8 NOT NULL,
# 	CONSTRAINT pk_relacionamento_tag_user_story PRIMARY KEY (id_tag, id_user_story),
# 	CONSTRAINT fk_relacionamento_tu_tag FOREIGN KEY (id_tag) REFERENCES public.dim_tag(id) ON DELETE CASCADE,
# 	CONSTRAINT fk_relacionamento_tu_user_story FOREIGN KEY (id_user_story) REFERENCES public.dim_user_story(id) ON DELETE CASCADE
# );

def upsert_relation_tag_us():
    Logger.info("Starting upsert_relation_tag_us process")

    db = Database()
    projects = get_all_projects()
    Logger.info(f"Retrieved {len(projects)} projects from the taiga.")
    
    select_internal_user_story_id = (
        "SELECT id FROM public.dim_user_story WHERE id_taiga = CAST(%s AS BIGINT)"
    )
    
    insert_relation = (
        "INSERT INTO public.relacionamento_tag_user_story (id_tag, id_user_story) "
        "VALUES (%s, %s) ON CONFLICT (id_tag, id_user_story) DO NOTHING"
    )
    
    select_relation = (
        "SELECT id_tag, id_user_story FROM public.relacionamento_tag_user_story "
        "WHERE id_tag = %s AND id_user_story = %s"
    )
    
    select_id_tag = "SELECT id FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)"
    
    conn = db.get_connection()
    Logger.info("Database connection established.")
    try:
        for project in projects:
            Logger.info(f"Processing project: {project['name']} (ID: {project['id']})")
            stories = get_user_storys_by_project(project["id"])
            Logger.info(
                f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
            )
            for story in stories:
                Logger.info(f"Processing user story ID: {story['id']}")
                for tag in story["tags"]:
                    Logger.info(
                        f"Processing tag '{tag[0]}' from user story ID: {story['id']}"
                    )
                    cursor = conn.cursor()
                    try:
                        Logger.debug(f"Executing query to find tag ID: {select_id_tag} with parameter: {tag[0]}")
                        cursor.execute(select_id_tag, (tag[0],))
                        internal_id_tag = cursor.fetchone()
                        if not internal_id_tag:
                            Logger.warning(
                                f"Tag '{tag[0]}' not found in the database. Skipping."
                            )
                            continue

                        Logger.debug(f"Executing query to find user story ID: {select_internal_user_story_id} with parameter: {story['id']}")
                        cursor.execute(select_internal_user_story_id, (story["id"],))
                        internal_user_story_id = cursor.fetchone()
                        if not internal_user_story_id:
                            Logger.warning(
                                f"User story '{story['id']}' not found in the database. Skipping."
                            )
                            continue

                        Logger.debug(f"Checking if relationship exists with query: {select_relation} and parameters: {internal_id_tag[0]}, {internal_user_story_id[0]}")
                        cursor.execute(select_relation, (internal_id_tag[0], internal_user_story_id[0]))
                        existing_relation = cursor.fetchone()
                        if not existing_relation:
                            Logger.debug(f"Inserting relationship with query: {insert_relation} and parameters: {internal_id_tag[0]}, {internal_user_story_id[0]}")
                            cursor.execute(insert_relation, (internal_id_tag[0], internal_user_story_id[0]))
                            conn.commit()
                            Logger.info(
                                f"Inserted relationship between tag '{tag[0]}' and user story ID: {story['id']}"
                            )
                        else:
                            Logger.info(
                                f"Relationship between tag '{tag[0]}' and user story ID: {story['id']}' already exists."
                            )
                    except Exception as e:
                        Logger.error(
                            f"Error processing tag '{tag[0]}' from user story ID: {story['id']}: {e}"
                        )
                    finally:
                        Logger.debug("Closing cursor.")
                        cursor.close()
    except Exception as e:
        Logger.error(f"Error processing projects: {e}")
    finally:
        Logger.info("Releasing database connection.")
        db.release_connection(conn)
    Logger.info("Finished upsert_relation_tag_us process.")

# def upsert_all_tags():
#     Logger.info("Starting upsert_all_tags process")

#     db = Database()
#     projects = get_all_projects()
#     Logger.info(f"Retrieved {len(projects)} projects from the database.")

#     select_tag_query = "SELECT * FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)"
#     insert_tag_query = "INSERT INTO public.dim_tag (nome) VALUES (LOWER(%s))"

#     conn = db.get_connection()
#     try:
#         for project in projects:
#             stories = get_user_storys_by_project(project["id"])
#             Logger.info(
#                 f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})"
#             )
#             for story in stories:
#                 for tag in story["tags"]:
#                     Logger.info(
#                         f"Processing tag '{tag[0]}' from user story ID: {story['id']}"
#                     )
#                     cursor = conn.cursor()
#                     try:
#                         cursor.execute(select_tag_query, (tag[0],))
#                         result = cursor.fetchone()
#                         if not result:
#                             Logger.info(
#                                 f"Tag '{tag[0]}' not found in database. Inserting new tag."
#                             )
#                             cursor.execute(insert_tag_query, (tag[0],))
#                             conn.commit()
#                         else:
#                             Logger.info(
#                                 f"Tag '{tag[0]}' already exists in the database."
#                             )
#                     except Exception as e:
#                         Logger.error(
#                             f"Error processing tag '{tag[0]}' from user story ID: {story['id']}: {e}"
#                         )
#                     finally:
#                         cursor.close()
#     except Exception as e:
#         Logger.error(f"Error processing projects: {e}")
#     finally:
#         db.release_connection(conn)

#     Logger.info(f"Processed all projects. Total projects: {len(projects)}")
#     return len(projects)
