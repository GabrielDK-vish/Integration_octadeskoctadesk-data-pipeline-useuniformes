import config
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
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
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
octa_headers = {
    "Content-Type":      "application/json",
    "Accept":            "application/json",
    "x-api-key":         OCTA_API_KEY,
    "octa-agent-email":  OCTA_AGENT_EMAIL,
}
config_path = Path(__file__).parent / "config.json"
with open(config_path) as f:
    config = json.load(f)

# Define o timezone BRT 
br_tz = timezone(timedelta(hours=-3))
# Define o fim do período como o momento "agora" no fuso BRT, removendo microssegundos
end_dt   = datetime.now(br_tz).replace(microsecond=0)
#start_dt = datetime(2024, 1, 1, tzinfo=br_tz)
start_dt = datetime.now(br_tz) - timedelta(days=1)
# Usamos a função split_windows, que retorna uma lista de tuplas (início, fim) para cada janela
windows = split_windows(start_dt, end_dt, timedelta(days=7))

df_ticket = fetch_all_tickets(start_dt, end_dt)

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

df_ticket_filtro1 = df_ticket[list(rename_map.keys())].rename(columns=rename_map)
df_custom_ticket = extrair_custom_ticket(df_ticket_filtro1)
df_ticket_final = df_ticket_filtro1.merge(df_custom_ticket, on="uuid", how="left")
   
df_chat = fetch_all_conversations(
    start_dt,
    end_dt,
    base_url=OCTA_BASE_URL,
    headers=octa_headers,
    limit=100,
    max_retries=3
)
df_verify_ticket = find_ticket(df_chat)

df_chat_final = coleta_chat(
    df_verify_ticket, 
    base_url=OCTA_BASE_URL, 
    headers=octa_headers
    )

df_chat_final = formatar_coluna1(df_chat_final)

df_chat_final['evt_ticket_ticketNumber'] = df_chat_final['evt_ticket_ticketNumber'].astype(str)
df_ticket_final['n_ticket'] = df_ticket_final['n_ticket'].astype(str)

df_upload = merge_ou_concat_campo_ticket(
    df_chat_final,
    df_ticket_final
)

df_upload['uuid'] = [str(uuid.uuid4()) for _ in range(len(df_upload))]
fuso_brasilia = pytz.timezone('America/Sao_Paulo')
data_hora_atual = datetime.now(fuso_brasilia)
df_upload['upload'] = data_hora_atual

creds = service_account.Credentials.from_service_account_file(config_path)
client = bigquery.Client(credentials=creds, project=creds.project_id)
df_upload["chat_id"] = df_upload["chat_id"].astype(str)

table_id = "integracoes-infinit.DataLake_2025.Sac_Octadesk"
try:
    client.get_table(table_id)
except NotFound:
    schema = [
        bigquery.SchemaField("chat_id", "STRING"),
        bigquery.SchemaField("n_ticket", "STRING"),
    ]
    client.create_table(bigquery.Table(table_id, schema=schema))

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
)

job = client.load_table_from_dataframe(df_upload, table_id, job_config=job_config)
job.result()

print("Upload feito")

