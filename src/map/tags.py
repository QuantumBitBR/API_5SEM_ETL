import logging

from src.config.database import Database
from src.extract.projects import get_all_projects
from src.extract.user_storys import get_user_storys_by_project
from src.utils.logger import Logger


def upsert_all_tags():
   Logger.info("Starting upsert_all_tags process")
   
   db = Database()
   projects = get_all_projects()
   Logger.info(f"Retrieved {len(projects)} projects from the database.")

   select_tag_query = "SELECT * FROM public.dim_tag WHERE LOWER(nome) = LOWER(%s)"
   insert_tag_query = "INSERT INTO public.dim_tag (nome) VALUES (LOWER(%s))"

   conn = db.get_connection()
   try:
      for project in projects:
         stories = get_user_storys_by_project(project["id"])
         Logger.info(f"Extracted {len(stories)} user stories from project {project['name']} (ID: {project['id']})")
         for story in stories:
            for tag in story["tags"]:
               Logger.info(f"Processing tag '{tag[0]}' from user story ID: {story['id']}")
               cursor = conn.cursor()
               try:
                  cursor.execute(select_tag_query, (tag[0],))
                  result = cursor.fetchone()
                  if not result:
                     Logger.info(f"Tag '{tag[0]}' not found in database. Inserting new tag.")
                     cursor.execute(insert_tag_query, (tag[0],))
                     conn.commit()
                  else:
                     Logger.info(f"Tag '{tag[0]}' already exists in the database.")
               except Exception as e:
                  Logger.error(f"Error processing tag '{tag[0]}' from user story ID: {story['id']}: {e}")
               finally:
                  cursor.close()
   except Exception as e:
      Logger.error(f"Error processing projects: {e}")
   finally:
      db.release_connection(conn)

   Logger.info(f"Processed all projects. Total projects: {len(projects)}")
   return len(projects)