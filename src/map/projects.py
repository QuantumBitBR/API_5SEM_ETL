from src.config.database import Database
from src.extract.projects import get_all_projects

# upsert_all_projects find all projects and insert them in the database (pk is name)

def upsert_all_projects():
   
   db = Database()
   projects = get_all_projects()
   
   for project in projects:
      conn = db.get_connection()
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM public.dim_projeto WHERE LOWER(name) = LOWER(%s)", (project["name"],))
      result = cursor.fetchone()
      if not result:
         cursor.execute("INSERT INTO public.dim_projeto (name) VALUES (LOWER(%s))", (project["name"],))
         conn.commit()
      cursor.close()
      db.release_connection(conn)
   return len(projects)