from extract.projects import get_projects


def main():
    """Executa a extração de dados do Taiga."""
    projects = get_projects()
    print(f"Extraídos {len(projects)} projetos.")
    print("=" * 40)
    for project in projects:
        print(project)
        print("-" * 40)

if __name__ == "__main__":
    main()