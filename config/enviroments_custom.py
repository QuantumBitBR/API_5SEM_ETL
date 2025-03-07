import os

from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

# API Taiga
TAIGA_API_URL = os.getenv("TAIGA_API_URL")
TAIGA_USERNAME = os.getenv("TAIGA_USERNAME")
TAIGA_PASSWORD = os.getenv("TAIGA_PASSWORD")

# Banco de Dados
DATABASE_URL = os.getenv("DATABASE_URL")