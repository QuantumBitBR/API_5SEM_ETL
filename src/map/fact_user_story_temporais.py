import datetime

from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.extract.users import get_user_by_id
from src.utils.logger import Logger

DIM_PERIODOS = [
    {"id": 1, "nome": "último dia"},
    {"id": 2, "nome": "último mês"},
    {"id": 3, "nome": "último ano"},
]


class FactUserStoryTemporais:
    id_usuario: int
    id_periodo: int
    id_projeto: int
    quantidade_user_stories_criadas: int
    quantidade_user_stories_finalizadas: int


def process_data_2_fact_temporais():

    select_internal_project_id = (
        "SELECT id FROM public.dim_projeto WHERE LOWER(nome) = LOWER(%s)"
    )
    select_internal_user_id = (
        "SELECT id FROM public.dim_usuario WHERE LOWER(email) = LOWER(%s)"
    )
    select_internal_user_story_id = (
        "SELECT id FROM public.dim_user_story WHERE id_taiga = CAST(%s AS BIGINT)"
    )

    now = datetime.datetime.now()  # Calculate 'now' once outside the loop

    eficiencia = {}
    conn = Database().get_connection()
    cursor = conn.cursor()

    try:
        projects = get_all_projects()
        for projetc in projects:

            # buscar o id do projeto
            cursor.execute(select_internal_project_id, (projetc["name"],))
            internal_project_id = cursor.fetchone()[0]

            Logger.info(
                f"Fetched internal project ID: {internal_project_id} for project {projetc['name']}"
            )

            stories = get_user_storys_by_project(projetc["id"])

            for story in stories:

                created_date = datetime.datetime.strptime(
                    story["created_date"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                finish_date = story["finish_date"]
                if finish_date is not None:
                    finish_date = datetime.datetime.strptime(
                        finish_date, "%Y-%m-%dT%H:%M:%S.%fZ"
                    )

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
                cursor.execute(select_internal_user_id, (user_complete["email"],))
                internal_user_id = cursor.fetchone()[0]

                Logger.info(
                    f"Fetched internal user ID: {internal_user_id} for user {taiga_user_id}"
                )

                for periodos in DIM_PERIODOS:

                    internal_key = (
                        f"{internal_user_id}-{periodos['id']}-{internal_project_id}"
                    )

                    if internal_key in eficiencia:
                        temp = eficiencia[internal_key]
                    else:
                        temp = FactUserStoryTemporais()
                        temp.id_usuario = internal_user_id
                        temp.id_periodo = periodos["id"]
                        temp.id_projeto = internal_project_id
                        temp.quantidade_user_stories_criadas = 0
                        temp.quantidade_user_stories_finalizadas = 0
                        eficiencia[internal_key] = temp

                    # Check if the task was created or finalized in the period in question
                    now = datetime.datetime.now()

                    if periodos["id"] == 1:  # último dia
                        start_period = now - datetime.timedelta(days=1)
                    elif periodos["id"] == 2:  # último mês
                        start_period = now - datetime.timedelta(days=30)
                    elif periodos["id"] == 3:  # último ano
                        start_period = now - datetime.timedelta(days=365)
                    else:
                        Logger.warning(
                            f"Unknown period ID: {periodos['id']}... skipping"
                        )
                        continue

                    Logger.info(
                        f"Period ID: {periodos['id']}, Start Period: {start_period}, Created Date: {created_date}, Finish Date: {finish_date}"
                    )
                    if created_date >= start_period:
                        temp.quantidade_user_stories_criadas += 1
                    if finish_date is not None and finish_date >= start_period:
                        temp.quantidade_user_stories_finalizadas += 1

        # Clear all data from the table before processing
        truncate_query = (
            "TRUNCATE TABLE public.fato_user_story_temporais RESTART IDENTITY CASCADE"
        )
        cursor.execute(truncate_query)
        Logger.info("Cleared all data from public.fato_user_story_temporais")

        for key, temp in eficiencia.items():
            # Check if the record already exists
            select_query = """
            SELECT quantidade_user_stories_criadas, quantidade_user_stories_finalizadas 
            FROM public.fato_user_story_temporais 
            WHERE id_usuario = %s AND id_periodo = %s AND id_projeto = %s
            """
            cursor.execute(
                select_query, (temp.id_usuario, temp.id_periodo, temp.id_projeto)
            )
            result = cursor.fetchone()

            if result:
                # Update the existing record
                update_query = """
                UPDATE public.fato_user_story_temporais 
                SET quantidade_user_stories_criadas = %s, 
                    quantidade_user_stories_finalizadas = %s
                WHERE id_usuario = %s AND id_periodo = %s AND id_projeto = %s
                """
                cursor.execute(
                    update_query,
                    (
                        temp.quantidade_user_stories_criadas,
                        temp.quantidade_user_stories_finalizadas,
                        temp.id_usuario,
                        temp.id_periodo,
                        temp.id_projeto,
                    ),
                )
            else:
                # Insert a new record
                insert_query = """
                INSERT INTO public.fato_user_story_temporais (
                    id_usuario, id_periodo, id_projeto, 
                    quantidade_user_stories_criadas, quantidade_user_stories_finalizadas
                ) VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(
                    insert_query,
                    (
                        temp.id_usuario,
                        temp.id_periodo,
                        temp.id_projeto,
                        temp.quantidade_user_stories_criadas,
                        temp.quantidade_user_stories_finalizadas,
                    ),
                )

        conn.commit()
        cursor.close()
        Database().release_connection(conn)
    except Exception as e:
        Logger.error(f"An error occurred while processing data: {e}")
        cursor.close()
        Database().release_connection(conn)
        return
