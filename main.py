from src.config.database import Database
from src.map.projects import upsert_all_projects
from src.map.tags import upsert_all_tags
from src.utils.logger import Logger


def main():
    """main."""

    
    Logger.initialize("etl_process")

    db = Database()
    print(db.health_check())

    upsert_all_projects()
    upsert_all_tags()
    
    Logger.info("Finished ETL process")


if __name__ == "__main__":
    main()
