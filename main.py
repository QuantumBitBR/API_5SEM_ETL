from dotenv import load_dotenv

from src.config import enviroments_custom
from src.config.database import Database
from src.map.fact_eficiencia_user_story import process_data_2_fact_eficiencia
from src.map.fact_status_user_story import process_fact_status_user_story
from src.map.fact_tag_user_story import process_fact_tag_user_story
from src.map.fact_user_story_temporais import process_data_2_fact_temporais
from src.map.projects import upsert_all_projects
from src.map.relation_project_user import upsert_relation_project_user
from src.map.relation_tag_user_story import upsert_relation_tag_us
from src.map.status import upsert_all_status
from src.map.tags import upsert_all_tags
from src.map.user_stories import upsert_all_user_stories
from src.map.users import upsert_all_users
from src.utils.logger import Logger


def main():
    """main."""
    

    Logger.initialize("etl_process")

    print(Database().health_check())
    
    Logger.info("Starting ETL process")
    Logger.info("-----------------------------------------------")

    Logger.info("Logging all environments:")
    Logger.info(f"TAIGA_API_URL: {enviroments_custom.TAIGA_API_URL}")
    Logger.info(f"TAIGA_API_USERNAME: {enviroments_custom.TAIGA_USERNAME}")
    
    Logger.info("-----------------------------------------------")
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
    upsert_relation_tag_us()
    Logger.info("-----------------------------------------------")
    upsert_relation_project_user()
    Logger.info("-----------------------------------------------")

    Database().close_pool()

    Logger.info("Finished ETL process")


if __name__ == "__main__":
    main()
