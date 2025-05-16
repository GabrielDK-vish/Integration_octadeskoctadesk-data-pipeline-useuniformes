import sys
import json
import pandas as pd
import uuid
import pytz
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
from manutencao import duplicidade_no_df
from config import OCTA_BASE_URL, OCTA_HEADERS, CONFIG_PATH, SRC_TABLE_SAC_OCTADESK, BQ
from ticket import (
    format_iso,
    split_windows,
    fetch_all_tickets,
    extrair_custom_ticket,
    update_ticket_status_by_ticket_id
)
from chat import (
    merge_ou_concat_campo_ticket,
    padronizar_col,
    fetch_all_chats
)


with open(CONFIG_PATH) as f:
    config = json.load(f)

# Define o timezone BRT 
br_tz = timezone(timedelta(hours=-3))
# Define o fim do período como o momento "agora" no fuso BRT, removendo microssegundos
end_dt   = datetime.now(br_tz).replace(microsecond=0)
#start_dt = datetime(2024, 1, 1, tzinfo=br_tz)
start_dt = datetime.now(br_tz) - timedelta(days=5)
# Usamos a função split_windows, que retorna uma lista de tuplas (início, fim) para cada janela
windows = split_windows(start_dt, end_dt, timedelta(days=7))

df_ticket = fetch_all_tickets(start_dt, end_dt)
#print(df_ticket.columns.tolist())
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

df_chat = fetch_all_chats(
    start_dt,
    end_dt,
    base_url=OCTA_BASE_URL,
    headers=OCTA_HEADERS,
    limit=100,
    max_retries=3
)

if df_ticket.empty and df_chat.empty:
    print("Nenhum dado, interrompendo execução.")
    sys.exit(0)

if df_ticket.empty and not df_chat.empty:
    print("df_ticket vazio")
    df_ticket = pd.DataFrame(columns=list(rename_map.keys()))

if df_chat.empty and not df_ticket.empty:
    print("df_chat vazio")
    df_chat = pd.DataFrame(columns=['number'])

for col in rename_map.keys():
    if col not in df_ticket.columns:
        df_ticket[col] = pd.NA

df_ticket_filtro1 = df_ticket[list(rename_map.keys())].rename(columns=rename_map)
df_custom_ticket = extrair_custom_ticket(df_ticket_filtro1)
df_ticket_final = df_ticket_filtro1.merge(df_custom_ticket, on="uuid", how="left")
   
if 'contact_cf_n_mero_do_ticket	' not in df_chat:
    df_chat['contact_cf_n_mero_do_ticket'] = ''
    
df_chat['number'] = df_chat['number'].astype(str)
df_chat['contact_cf_n_mero_do_ticket'] = df_chat['contact_cf_n_mero_do_ticket'].astype(str)
df_ticket_final['n_ticket'] = df_ticket_final['n_ticket'].astype(str)

df_upload = merge_ou_concat_campo_ticket(
    df_chat,
    df_ticket_final
)


df_upload['uuid'] = df_upload['uuid'].apply(
    lambda x: x if pd.notna(x) and str(x).strip() != '' else str(uuid.uuid4())
)

fuso_brasilia = pytz.timezone('America/Sao_Paulo')
data_hora_atual = datetime.now(fuso_brasilia)
df_upload['upload'] = data_hora_atual


creds = service_account.Credentials.from_service_account_file(CONFIG_PATH)
client = bigquery.Client(credentials=creds, project=creds.project_id)
df_upload["id"] = df_upload["id"].astype(str)

table_id = "integracoes-infinit.DataLake_2025.Octadesk"

df_upload = padronizar_col(df_upload)

df_upload = duplicidade_no_df(df_upload, table_id)

df_upload = df_upload.loc[:, ~df_upload.columns.duplicated()].copy()

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

sql = f"""
SELECT DISTINCT n_ticket
FROM {SRC_TABLE_SAC_OCTADESK}
WHERE (n_ticket is not null) AND (status_ticket != 'Resolvido')
"""

df_tabela = BQ.query(sql).to_dataframe()
tickets_list = df_tabela["n_ticket"].tolist()

for ticket in tickets_list:
    print(update_ticket_status_by_ticket_id(ticket))
