from src.config.database import Database
from src.map.fact_eficiencia_user_story import process_data_2_fact_eficiencia
from src.map.fact_status_user_story import process_fact_status_user_story
from src.map.fact_tag_user_story import process_fact_tag_user_story
from src.map.fact_user_story_temporais import process_data_2_fact_temporais
from src.map.projects import upsert_all_projects
from src.map.status import upsert_all_status
from src.map.tags import upsert_all_tags
from src.map.user_stories import upsert_all_user_stories
from src.map.users import upsert_all_users
from src.utils.logger import Logger


def main():
    """main."""

    Logger.initialize("etl_process")

    print(Database().health_check())

    upsert_all_projects()
    Logger.info("-----------------------------------------------")
    upsert_all_users()
    Logger.info("-----------------------------------------------")
    upsert_all_status()
    Logger.info("-----------------------------------------------")
    upsert_all_tags()
    Logger.info("-----------------------------------------------")
    upsert_all_user_stories()
    Logger.info("-----------------------------------------------")
    process_fact_status_user_story()
    Logger.info("-----------------------------------------------")
    process_fact_tag_user_story()
    Logger.info("-----------------------------------------------")
    process_data_2_fact_eficiencia()
    Logger.info("-----------------------------------------------")
    process_data_2_fact_temporais()
    Logger.info("-----------------------------------------------")

    Logger.info("Finished ETL process")


if __name__ == "__main__":
    main()
