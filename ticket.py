import requests 
import json
import pandas as pd
from google.cloud import bigquery
from time import sleep  
from datetime import datetime, timedelta 
from typing import List, Tuple
from config import OCTA_BASE_URL, OCTA_HEADERS, BQ, SRC_TABLE_SAC_OCTADESK, TIMEZONE

def fetch_octadesk_tickets(params: dict) -> pd.DataFrame:
    url  = f"{OCTA_BASE_URL}/tickets"
    resp = requests.get(url, headers=OCTA_HEADERS, params=params)
    if resp.status_code != 200:
        print(f"Erro status: {resp.status_code}\n{resp.text}")
        resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and "results" in payload:
        data = payload["results"]
    elif isinstance(payload, list):
        data = payload
    else:
        data = []
    return pd.json_normalize(data)


def format_iso(dt: datetime) -> str:
    """Formata datetime como 'YYYY-MM-DDTHH:MM:SS±HHMM' (sem ':')."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


def split_windows(start: datetime, end: datetime, delta: timedelta) -> List[Tuple[datetime, datetime]]:
    windows = []
    cur = start
    while cur < end:
        nxt = min(cur + delta, end)
        windows.append((cur, nxt))
        cur = nxt
    return windows


def fetch_tickets_with_split(start: datetime,
                             end: datetime,
                             min_delta: timedelta = timedelta(hours=1),
                             limit: int = 100) -> pd.DataFrame:
    s_iso = format_iso(start)
    e_iso = format_iso(end)

    try:
        return fetch_octadesk_tickets({
            "page":  1,
            "limit": limit,
            "filters[0][property]": "createdAt",
            "filters[0][operator]": "ge",
            "filters[0][value]": s_iso,
            "filters[1][property]": "createdAt",
            "filters[1][operator]": "le",
            "filters[1][value]": e_iso
        })
    except requests.exceptions.HTTPError as err:
        code = err.response.status_code if err.response else None
        if code and 500 <= code < 600 and (end - start) > min_delta:
            mid   = start + (end - start) / 2
            left  = fetch_tickets_with_split(start, mid,  min_delta, limit)
            right = fetch_tickets_with_split(mid,   end,  min_delta, limit)
            return pd.concat([left, right], ignore_index=True)
        print(f"Pulando janela {s_iso}→{e_iso}: {err}")
        return pd.DataFrame()

def extrair_custom_ticket(df_ticket_filtro1: pd.DataFrame) -> pd.DataFrame:
    
    keys = [
        "codigo_de_rastreio", "cpf", "data_de_pagamento",
        "email_do_cliente", "motivo_de_contatos",
        "n_da_nota_fiscal", "n_do_pedido",
        "n_do_pedido_bling", "produto", "tipo_do_problema"
    ]
    
    def filtrar_custom(lista):
        return {
            item['key']: item['value']
            for item in lista
            if item['key'] in keys
        }  
    
    custom_dicts = df_ticket_filtro1['campo_custom_ticket'].apply(filtrar_custom)  
    custom_df = pd.DataFrame(custom_dicts.tolist())                                  

    rename_map = {k: f"ticket_{k}" for k in custom_df.columns}                      
    custom_df = custom_df.rename(columns=rename_map)                                

    resultado = pd.concat(
        [df_ticket_filtro1[['uuid']].reset_index(drop=True), custom_df],
        axis=1
    )  

    return resultado

def fetch_all_tickets(start_dt: datetime, end_dt: datetime,
                      limit: int = 100, max_retries: int = 3) -> pd.DataFrame:
    all_tickets = []
    page = 1

    # Remove microssegundos e força ISO sem frações
    start_iso = start_dt.replace(microsecond=0).isoformat()
    end_iso   = end_dt  .replace(microsecond=0).isoformat()

    while True:
        # Monta filtros como array de objetos
        params = {
            "filters[0][property]": "createdAt",
            "filters[0][operator]": "ge",
            "filters[0][value]":    start_iso,
            "filters[1][property]": "createdAt",
            "filters[1][operator]": "le",
            "filters[1][value]":    end_iso,
            "page":                 page,
            "limit":                limit,
            "sort[property]":       "createdAt",
            "sort[direction]":      "asc"
        }

        # Retry em caso de 409/500 ou outros HTTPError
        for attempt in range(1, max_retries + 1):
            resp = requests.get(f"{OCTA_BASE_URL}/tickets",
                                headers=OCTA_HEADERS,
                                params=params)
            print(f"Tentativa {attempt} — status {resp.status_code}")  # ajuda no debug
            if resp.status_code in (409, 500):
                sleep(2 ** (attempt - 1))
                continue
            try:
                resp.raise_for_status()
            except requests.HTTPError:
                if attempt == max_retries:
                    print(f"Todas as tentativas falharam na página {page}.")
                    return pd.DataFrame()
                continue
            break

        data = resp.json()
        if not data:
            break

        all_tickets.extend(data)
        if len(data) < limit:
            break
        page += 1

    return pd.json_normalize(all_tickets)

def update_ticket_status_by_ticket_id(ticket_id: str) -> str:
    try:
        # 1. Busca dados do ticket na API Octadesk
        resp = requests.get(f"{OCTA_BASE_URL}/tickets/{ticket_id}", headers=OCTA_HEADERS)
        resp.raise_for_status()
        data = resp.json()

        # 2. Extrai campos customizados e status
        custom = {item["key"]: item["value"] for item in data.get("customField", [])}
        tipo_produto   = custom.get("produto")
        n_pedido       = custom.get("n_do_pedido")
        n_pedido_bling = custom.get("n_do_pedido_bling")
        tags_list      = data.get("tags", [])  # Lista de strings do Python
        cpf            = custom.get("cpf")
        status1        = data.get("status", {}).get("name")
        status2        = (
            data.get("lastHumanInteraction", {})
                .get("propertiesChanges", {})
                .get("status")
        )

        # 3. Monta a query usando parâmetro de array
        sql = f"""
        UPDATE `{SRC_TABLE_SAC_OCTADESK}`
        SET
          ticket_produto           = @tipo_produto,
          ticket_n_do_pedido       = @n_pedido,
          ticket_n_do_pedido_bling = @n_pedido_bling,
          tags                     = @tags,
          ticket_cpf               = @cpf,
          status_ticket            = @status1,
          status_ticket2           = @status2
        WHERE n_ticket = @ticket_id
        """

        # 4. Prepara parâmetros, incluindo ArrayQueryParameter para tags
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tipo_produto", "STRING", tipo_produto),
                bigquery.ScalarQueryParameter("n_pedido", "STRING", n_pedido),
                bigquery.ScalarQueryParameter("n_pedido_bling", "STRING", n_pedido_bling),
                bigquery.ArrayQueryParameter("tags", "STRING", tags_list),
                bigquery.ScalarQueryParameter("cpf", "STRING", cpf),
                bigquery.ScalarQueryParameter("status1", "STRING", status1),
                bigquery.ScalarQueryParameter("status2", "STRING", status2),
                bigquery.ScalarQueryParameter("ticket_id", "STRING", ticket_id),
            ]
        )  # :contentReference[oaicite:2]{index=2}

        # 5. Executa a query e aguarda a conclusão
        query_job = BQ.query(sql, job_config=job_config)
        query_job.result()

        # 6. Retorna confirmação com timestamp
        date = datetime.now(TIMEZONE)
        return f"Update realizado com sucesso para o ticket {ticket_id} - {date}"

    except requests.RequestException as e:
        return f"Erro na requisição à API Octadesk: {e}"
    except Exception as e:
        # Captura erros do BigQuery, incluindo tipo de parâmetro incorreto
        return f"Erro ao atualizar ticket {ticket_id}: {e}"
