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

    insert_user_story = "INSERT INTO public.dim_user_story (id_taiga, assunto, criado_em, finalizado_em, bloqueado, encerrado, data_limite, id_status, id_usuario, id_projeto, id_sprint) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    update_user_story = "UPDATE public.dim_user_story SET assunto = %s, criado_em = %s, finalizado_em = %s, bloqueado = %s, encerrado = %s, data_limite = %s, id_status = %s, id_usuario = %s, id_projeto = %s, id_sprint = %s WHERE id_taiga = %s"

    select_sprint_id_internal = (
        "SELECT id FROM public.dim_sprint WHERE LOWER(nome) = LOWER(%s)"
    )
    insert_sprint = "INSERT INTO public.dim_sprint (nome) VALUES (%s)"

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
                    retrabalho(story, conn, cursor)
                except Exception as e:
                    Logger.error(
                        f"Error processing rework for story {story['id']}: {e}"
                    )

                Logger.info(
                    f"Processing story {story['id']} from project {project['name']}"
                )

                try:

                    sprint_id_internal = None
                    if story["milestone_name"] is not None:
                        cursor = conn.cursor()
                        cursor.execute(
                            select_sprint_id_internal, (story["milestone_name"],)
                        )
                        sprint_id_internal = cursor.fetchone()[0]

                        if not sprint_id_internal:
                            cursor.execute(insert_sprint, (story["milestone_name"],))
                            conn.commit()
                            Logger.info(f"Inserted sprint {story['milestone_name']}")
                            cursor.execute(
                                select_sprint_id_internal, (story["milestone_name"],)
                            )
                            sprint_id_internal = cursor.fetchone()[0]
                            Logger.info(f"Fetched sprint ID: {sprint_id_internal}")
                        else:
                            Logger.info(f"Fetched sprint ID: {sprint_id_internal}")

                    cursor.execute(select_user_story_by_taiga_id, (story["id"],))
                    story_in_bd = cursor.fetchone()

                    cursor.execute(
                        select_status_id_internal, (story["status_extra_info"]["name"],)
                    )
                    status_id_internal = cursor.fetchone()

                    assigned_to_extra_info = story.get("assigned_to_extra_info")
                    taiga_user_id = (
                        assigned_to_extra_info.get("id")
                        if assigned_to_extra_info
                        else None
                    )
                    if taiga_user_id is None:
                        Logger.warning(
                            f"Assigned user is None for story {story['id']}... skipping"
                        )
                        continue

                    Logger.info(f"Fetching user details for Taiga ID: {taiga_user_id}")
                    user_complete = get_user_by_id(taiga_user_id, project["id"])
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
                                internal_project_id,
                                sprint_id_internal,
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
                                sprint_id_internal,
                                story["id"],
                            ),
                        )
                        conn.commit()
                        Logger.info(f"Updated story {story['id']}")
                except Exception as e:
                    Logger.error(f"Error processing status {story['id']}: {e}")
                # Do not close the cursor here; close it after all stories in the project are processed.

    except Exception as e:
        Logger.error(f"Error processing projects: {e}")
    finally:
        db.release_connection(conn)

    Logger.info(f"Processed all projects. Total projects: {len(projects)}")
    return len(projects)


def retrabalho(user_story, conn, cursor):
    """
    Function to calculate the rework of a user story.
    :param project: Project name
    :param user_story: User story details
    :return: Rework details
    """
    # Logica para retrabalho
    #     novo_status = status obtido da origem

    #     status_atual = SELECT status_atual FROM dim_user_story WHERE id_user_story = ?

    #     SE novo_status == status_atual:
    #         NÃO FAZ NADA

    #     SENÃO SE novo_status > status_atual:
    #         -- avanço de status
    #         UPDATE dim_user_story
    #         SET status_anterior = status_atual,
    #             status_atual = novo_status
    #         WHERE id_user_story = ?

    #     SENÃO SE novo_status < status_atual:
    #         -- retrabalho detectado
    #         INSERT INTO dim_historico_retrabalho (id_user_story, data_retrabalho, status_retrocedido)
    #         VALUES (?, NOW(), novo_status)

    #         UPDATE dim_user_story
    #         SET status_anterior = status_atual,
    #             status_atual = novo_status
    #         WHERE id_user_story = ?

    Logger.info("Starting user story rework process")

    status_weights = {
        "new": 1,
        "ready": 2,
        "in progress": 3,
        "ready for test": 4,
        "done": 5,
    }

    try:
        select_current_status = (
            "SELECT ds.tipo FROM public.dim_user_story dus "
            "JOIN public.dim_status ds ON ds.id = dus.id_status "
            "WHERE dus.id_taiga = CAST(%s AS BIGINT)"
        )
        cursor.execute(select_current_status, (user_story["id"],))
        current_status_row = cursor.fetchone()

        if current_status_row is None:
            Logger.warning(
                f"User story {user_story['id']} not found in the database. Skipping rework process."
            )
            return

        current_status_name = current_status_row[0] if current_status_row else None
        if current_status_name is not None:
            current_status_name = str(current_status_name).lower()

        new_status_name = user_story["status_extra_info"]["name"].lower()
        Logger.info(
            f"User story {user_story['id']} - Current status: {current_status_name}, New status: {new_status_name}"
        )

        if user_story["id"] == 7867302:
            print(
                f"User story {user_story['id']} - Current status: {current_status_name}, New status: {new_status_name}"
            )

        selec_internal_id_status = (
            "SELECT id FROM public.dim_status WHERE LOWER(tipo) = LOWER(%s)"
        )
        cursor.execute(selec_internal_id_status, (new_status_name,))
        internal_new_status_id = cursor.fetchone()[0]

        select_internal_user_story_id = (
            "SELECT id FROM public.dim_user_story WHERE id_taiga = CAST(%s AS BIGINT)"
        )
        cursor.execute(select_internal_user_story_id, (user_story["id"],))
        internal_user_story_id = cursor.fetchone()[0]

        if status_weights.get(new_status_name) == status_weights.get(
            current_status_name
        ):
            Logger.info(
                f"User story {user_story['id']} - Status unchanged. No action taken."
            )
            return

        if status_weights.get(new_status_name) > status_weights.get(
            current_status_name
        ):
            # Avanço de status
            Logger.info(
                f"User story {user_story['id']} - Status advanced from {current_status_name} to {new_status_name}. Updating database."
            )
            try:
                update_status = "UPDATE public.dim_user_story SET id_status = %s WHERE id_taiga = CAST(%s AS BIGINT)"
                cursor.execute(
                    update_status, (internal_new_status_id, user_story["id"])
                )
                conn.commit()
                Logger.info(
                    f"User story {user_story['id']} - Status updated successfully."
                )
            except Exception as e:
                Logger.error(
                    f"Error updating status for user story {user_story['id']}: {e}"
                )

        if status_weights.get(new_status_name) < status_weights.get(
            current_status_name
        ):
            # Retrabalho detectado
            Logger.warning(
                f"User story {user_story['id']} - Rework detected! Status regressed from {current_status_name} to {new_status_name}. Logging rework and updating database."
            )
            try:
                insert_rework = "INSERT INTO public.dim_status_historico (id_user_story, data_retrabalho, id_status) VALUES (CAST(%s AS BIGINT), NOW(), %s)"
                cursor.execute(
                    insert_rework, (internal_user_story_id, internal_new_status_id)
                )
                conn.commit()
                Logger.info(
                    f"User story {user_story['id']} - Rework logged in dim_status_historico."
                )
            except Exception as e:
                Logger.error(
                    f"Error logging rework for user story {user_story['id']}: {e}"
                )
            try:
                update_status = "UPDATE public.dim_user_story SET id_status = %s WHERE id_taiga = CAST(%s AS BIGINT)"
                cursor.execute(
                    update_status, (internal_new_status_id, user_story["id"])
                )
                conn.commit()
                Logger.info(
                    f"User story {user_story['id']} - Status updated after rework."
                )
            except Exception as e:
                Logger.error(
                    f"Error updating status after rework for user story {user_story['id']}: {e}"
                )
    except Exception as e:
        Logger.error(
            f"Error processing rework logic for user story {user_story['id']}: {e}"
        )
