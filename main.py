from config.database import Database
from extract.projects import get_projects
from extract.user_storys import get_user_storys_by_project


def main():
    """main."""

    db = Database()
    
    print(db.health_check())
    
    
    projects = get_projects()
    print(f"Extraídos {len(projects)} projetos.")
    for project in projects:
        print(project["name"])
        user_storys = get_user_storys_by_project(project["id"])
        print(f"Extraídos {len(user_storys)} user stories.")
        for user_story in user_storys:
            print(user_story)

        print("-" * 40)


if __name__ == "__main__":
    main()
