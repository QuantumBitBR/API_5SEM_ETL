from src.config.database import Database
from src.map.fact_status_user_storie import process_fact_status_user_storie
from src.map.fact_tag_user_storie import process_fact_tag_user_storie
from src.map.projects import upsert_all_projects
from src.map.status import upsert_all_status
from src.map.tags import upsert_all_tags
from src.utils.logger import Logger


def main():
    """main."""

    Logger.initialize("etl_process")

    db = Database()
    print(db.health_check())

    upsert_all_projects()
    Logger.info("-----------------------------------------------")
    upsert_all_status()
    Logger.info("-----------------------------------------------")
    upsert_all_tags()
    Logger.info("-----------------------------------------------")
    process_fact_status_user_storie()
    Logger.info("-----------------------------------------------")
    process_fact_tag_user_storie()
    Logger.info("-----------------------------------------------")

    Logger.info("Finished ETL process")


if __name__ == "__main__":
    main()
