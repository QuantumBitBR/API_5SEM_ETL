from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.users import get_all_users_by_project
from src.utils.logger import Logger


# upsert_all_projects find all projects and insert them in the database (pk is name)
def upsert_all_users():
    Logger.info("Starting upsert_all_users process")

    db = Database()

    projects = get_all_projects()

    Logger.info(f"Retrieved {len(projects)} projects from source")

    for project in projects:

        users = get_all_users_by_project(project["id"])
        Logger.info(f"Retrieved {len(users)} users from source")

        select_user_by_email = "SELECT * FROM public.dim_usuario WHERE email = %s"
        insert_user = "INSERT INTO public.dim_usuario (nome, email, role, is_enable) VALUES (%s, %s, %s, %s)"
        update_user = "UPDATE public.dim_usuario SET nome = %s WHERE email = %s"

        conn = db.get_connection()
        try:
            for user in users:
                cursor = conn.cursor()
                try:

                    if "email" not in user:
                        Logger.warning(
                            f"User data does not contain 'email' field: {user}"
                        )
                        continue
                    cursor.execute(select_user_by_email, (user["email"],))
                    user_in_bd = cursor.fetchone()

                    if user_in_bd is None:
                        cursor.execute(
                            insert_user,
                            (
                                user["full_name_display"],
                                user["email"],
                                "OPERADOR",
                                "false",
                            ),
                        )
                        conn.commit()
                        Logger.info(f"Inserted user {user['email']}")
                    else:
                        cursor.execute(
                            update_user, (user["full_name_display"], user["email"])
                        )
                        conn.commit()
                        Logger.info(f"Updated user {user['email']}")
                except Exception as e:
                    Logger.error(f"Error processing user {user['email']}: {e}")
                finally:
                    cursor.close()

        except Exception as e:
            Logger.error(f"Error processing users: {e}")
        finally:
            db.release_connection(conn)
        Logger.info("Completed upsert_all_users process")
    return
