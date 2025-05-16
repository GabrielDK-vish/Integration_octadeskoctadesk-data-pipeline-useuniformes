from dotenv import load_dotenv
from pathlib import Path
from google.oauth2 import service_account
from google.cloud import bigquery
import os
import pytz

dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

OCTA_BASE_URL    = os.getenv("OCTA_BASE_URL", "").rstrip("/")
OCTA_API_KEY     = os.getenv("OCTA_API_KEY", "")
OCTA_AGENT_EMAIL = os.getenv("OCTA_AGENT_EMAIL", "")

missing = [n for n,v in [
    ("OCTA_BASE_URL",    OCTA_BASE_URL),
    ("OCTA_API_KEY",     OCTA_API_KEY),
    ("OCTA_AGENT_EMAIL", OCTA_AGENT_EMAIL)
] if not v]
if missing:
    raise RuntimeError(f"Faltando variáveis no .env: {missing}")

CONFIG_PATH = Path(__file__).parent / "config.json"
CREDS       = service_account.Credentials.from_service_account_file(CONFIG_PATH)
PROJECT     = CREDS.project_id

if PROJECT is None:
    raise RuntimeError(f"ID do projeto não encontrado nas credenciais.")

BQ = bigquery.Client(credentials=CREDS, project=PROJECT)
TIMEZONE = pytz.timezone("America/Sao_Paulo")
SRC_TABLE_SAC_OCTADESK = f"{PROJECT}.DataLake_2025.Octadesk"
SRC_TABLE_TICKETS_ABERTOS = f"{PROJECT}.DataWareHouse_2025.Sac_TicketsAbertos"

OCTA_HEADERS = {
    "Content-Type":      "application/json",
    "Accept":            "application/json",
    "x-api-key":         OCTA_API_KEY,
    "octa-agent-email":  OCTA_AGENT_EMAIL,
}
