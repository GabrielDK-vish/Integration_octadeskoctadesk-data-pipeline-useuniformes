import config
import sys
import json
import pandas as pd
import requests
import uuid
import pytz
import logging
import os
from pandas import json_normalize
from datetime import datetime, timezone, timedelta
from google.oauth2 import service_account
from pathlib import Path
from requests.exceptions import HTTPError
from time import sleep
from typing import List, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import NotFound as GCPNotFound
from google.api_core.exceptions import NotFound as CoreNotFound

from manutencao import duplicidade_no_df
from config import OCTA_BASE_URL, OCTA_API_KEY, OCTA_AGENT_EMAIL
from ticket import (
    format_iso,
    split_windows,
    fetch_all_tickets,
    extrair_custom_ticket
)
from chat import (
    fetch_all_conversations,
    merge_ou_concat_campo_ticket,
    find_ticket,
    coleta_chat,
    formatar_coluna1
)

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# credenciais
config_path = Path(__file__).parent / "config.json"
with open(config_path) as f:
    config_data = json.load(f)

octa_headers = {
    "Content-Type":     "application/json",
    "Accept":           "application/json",
    "x-api-key":        OCTA_API_KEY,
    "octa-agent-email": OCTA_AGENT_EMAIL,
}

# Datas de início e fim
br_tz = pytz.timezone('America/Sao_Paulo')
start_date = datetime(2024, 4, 1).astimezone(br_tz).replace(microsecond=0)
end_date = datetime.now(br_tz).replace(microsecond=0)
delta = timedelta(weeks=1)
windows: List[Tuple[datetime, datetime]] = split_windows(start_date, end_date, delta)

# Cliente BigQuery
creds = service_account.Credentials.from_service_account_file(config_path)
bq_client = bigquery.Client(credentials=creds, project=creds.project_id)
table_id = "integracoes-infinit.DataLake_2025.Sac_Octadesk"

# Garante que a tabela existe
try:
    bq_client.get_table(table_id)
    logger.info(f"Tabela {table_id} já existe.")
except (GCPNotFound, CoreNotFound):
    logger.info(f"Tabela {table_id} não encontrada. Criando...")
    schema = [
        bigquery.SchemaField("chat_id", "STRING"),
        bigquery.SchemaField("n_ticket", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    bq_client.create_table(table)
    logger.info("Tabela criada com sucesso.")

# Processa cada janela semanal
for idx, (window_start, window_end) in enumerate(windows, start=1):
    logger.info(f"Processando janela {idx}/{len(windows)}: {window_start.isoformat()} a {window_end.isoformat()}")

    try:
        df_ticket = fetch_all_tickets(window_start, window_end)
        df_chat = fetch_all_conversations(
            window_start,
            window_end,
            base_url=OCTA_BASE_URL,
            headers=octa_headers,
            limit=100,
            max_retries=3
        )
    except HTTPError as e:
        logger.error(f"Erro ao buscar dados da API: {e.response.status_code} - {e.response.text}")
        continue

    if df_ticket.empty and df_chat.empty:
        logger.info("Nenhum dado encontrado nesta janela. Pulando.")
        continue

    if df_ticket.empty:
        df_ticket = pd.DataFrame(columns=[
            'id','number','summary','tags','createdAt','updatedAt',
            'status.name','channel.name','requester.name','requester.email',
            'group.id','lastHumanInteraction.propertiesChanges.status',
            'customField','requester.customField'
        ])
    if df_chat.empty:
        df_chat = pd.DataFrame(columns=["number"])

    rename_map = {
        'id': 'uuid',
        'number': 'n_ticket',
        'summary': 'titulo',
        'tags': 'tags_ticket',
        'createdAt': 'createdAt',
        'updatedAt': 'updatedAt',
        'status.name': 'status_ticket',
        'channel.name': 'channel_ticket',
        'requester.name': 'autor_ticket',
        'requester.email': 'email_ticket',
        'group.id': 'grupo_responsavel_ticket',
        'lastHumanInteraction.propertiesChanges.status': 'status_ticket2',
        'customField': 'campo_custom_ticket',
        'requester.customField': 'campo_custom_ticket2'
    }
    for col in rename_map:
        if col not in df_ticket.columns:
            df_ticket[col] = pd.NA

    df_ticket_sel = df_ticket[list(rename_map)].rename(columns=rename_map)
    df_custom = extrair_custom_ticket(df_ticket_sel)
    df_ticket_final = df_ticket_sel.merge(df_custom, on="uuid", how="left")

    df_verify = find_ticket(df_chat)
    df_chat_proc = coleta_chat(df_verify, base_url=OCTA_BASE_URL, headers=octa_headers)
    df_chat_proc = formatar_coluna1(df_chat_proc)
    if 'evt_ticket_ticketNumber' not in df_chat_proc:
        df_chat_proc['evt_ticket_ticketNumber'] = ''

    df_upload = merge_ou_concat_campo_ticket(df_chat_proc, df_ticket_final)
    df_upload['uuid'] = [str(uuid.uuid4()) for _ in range(len(df_upload))]
    df_upload['upload'] = datetime.now(br_tz)

    if 'chat_id' in df_upload.columns:
        df_upload['chat_id'] = df_upload['chat_id'].astype(str)
    else:
        logger.warning("Coluna 'chat_id' ausente em df_upload. Pulando a conversão de tipo.")

    df_upload = duplicidade_no_df(df_upload, table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    load_job = bq_client.load_table_from_dataframe(df_upload, table_id, job_config=job_config)
    load_job.result()
    logger.info(f"Janela {idx} carregada com {len(df_upload)} registros.")
    sleep(2)

logger.info("Processamento completo de todas as janelas semanais.")
