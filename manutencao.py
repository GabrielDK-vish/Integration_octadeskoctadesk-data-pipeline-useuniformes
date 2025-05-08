import json
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Carrega o config.json que está na mesma pasta deste script
config_path = Path(__file__).parent / "config.json"
with open(config_path, "r", encoding="utf-8") as f:
    config = json.load(f)

def duplicidade_no_df(df: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    # Cria cliente BigQuery usando credenciais do config.json
    key_info = config  # dict com as credenciais da service-account
    creds = service_account.Credentials.from_service_account_info(key_info)
    client = bigquery.Client(credentials=creds, project=creds.project_id)

    original_len = len(df)
    df_filtrado = df.copy()

    # Para cada coluna, faz consulta parametrizada de acordo com o tipo
    for coluna in ('number', 'n_ticket'):
        if coluna not in df_filtrado.columns:
            continue

        # Extrai valores únicos não-nulos
        valores = df_filtrado[coluna].dropna().unique().tolist()
        if not valores:
            continue

        # Detecta tipo de BigQuery e converte valores
        if coluna == 'number':
            param_type = 'INT64'
            valores = [int(v) for v in valores]
        else:
            param_type = 'STRING'

        # Monta a query
        query = f"""
            SELECT {coluna}
            FROM `{nome_tabela}`
            WHERE {coluna} IN UNNEST(@valores)
        """

        # Configura parâmetros tipados
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("valores", param_type, valores)
            ]
        )

        # Executa consulta e obtém valores existentes
        resultado = client.query(query, job_config=job_config).result()
        existentes = {row[coluna] for row in resultado}

        # Filtra DataFrame removendo duplicados
        df_filtrado = df_filtrado[~df_filtrado[coluna].isin(existentes)]

    removidas = original_len - len(df_filtrado)
    print(f"{removidas} linhas excluídas")

    return df_filtrado.reset_index(drop=True)
