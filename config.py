from dotenv import load_dotenv
from pathlib import Path
import os

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
