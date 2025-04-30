import requests
import pandas as pd
import logging
import re
from typing import Optional, Dict, Any, List
from datetime import datetime
from requests.exceptions import HTTPError
from time import sleep
from config import OCTA_BASE_URL, OCTA_API_KEY, OCTA_AGENT_EMAIL


octa_base_url = OCTA_BASE_URL
octa_headers = {
    "Content-Type":      "application/json",
    "Accept":            "application/json",
    "x-api-key":         OCTA_API_KEY,
    "octa-agent-email":  OCTA_AGENT_EMAIL,
}

#Padronizar colunas _________________________________________
def formatar_coluna2(name: str) -> str:
   
    clean = re.sub(r'[^0-9A-Za-z_]', '_', name)
    if re.match(r'^[0-9]', clean):
        clean = '_' + clean
    return clean[:300]

def formatar_coluna1(df: pd.DataFrame) -> pd.DataFrame:
    
    new_cols = {col: formatar_coluna2(col) for col in df.columns}
    return df.rename(columns=new_cols)

#_________________________________________________________________
def fetch_all_conversations(
    start_dt: datetime,
    end_dt: datetime,
    base_url: str,
    headers: dict,
    limit: int = 100,
    max_retries: int = 3
) -> pd.DataFrame:
    """
    Busca todas as conversas no intervalo [start_dt, end_dt] paginando resultados.

    Parâmetros:
    - start_dt: datetime de início.
    - end_dt: datetime de término.
    - base_url: URL base da API OctaDesk.
    - headers: headers HTTP para autenticação.
    - limit: número máximo de registros por página (até 100).
    - max_retries: número de tentativas em erros 409/500.

    Retorna:
    - DataFrame pandas com todas as conversas normalizadas.
    """
    start_iso = start_dt.replace(microsecond=0).isoformat(timespec='seconds')
    end_iso = end_dt.replace(microsecond=0).isoformat(timespec='seconds')

    # garante que não passe de 100
    limit = min(limit, 100)

    all_chats = []
    page = 1

    while True:
        params = {
            "filters[0][property]":  "createdAt",
            "filters[0][operator]":  "ge",
            "filters[0][value]":     start_iso,
            "filters[1][property]":  "createdAt",
            "filters[1][operator]":  "le",
            "filters[1][value]":     end_iso,
            "page":                  page,
            "limit":                 limit,
            "sort[property]":        "createdAt",
            "sort[direction]":       "asc"
        }

        # tenta até max_retries
        for attempt in range(1, max_retries + 1):
            resp = requests.get(f"{base_url}/chat", headers=headers, params=params)
            if resp.status_code in (409, 500):
                backoff = 2 ** (attempt - 1)
                print(f"⚠️ {resp.status_code} na página {page}, retry em {backoff}s (tentativa {attempt})")
                sleep(backoff)
                continue
            resp.raise_for_status()
            break

        data = resp.json()
        if isinstance(data, dict):
            chats = data.get("results", [])
        elif isinstance(data, list):
            chats = data
        else:
            chats = []

        if not chats:
            break

        all_chats.extend(chats)
        page += 1

    # enriquece com campos customizados
    enriched = []
    for chat in all_chats:
        rec = chat.copy()
        for fld in rec.get("customFields", []):
            name = fld.get("name") or fld.get("key")
            value = fld.get("value")
            if name:
                rec[f"cf_chat_{name}"] = value
        enriched.append(rec)

    # normaliza em DataFrame
    return pd.json_normalize(enriched)

# merge basico ticket e chat
def merge_ou_concat_campo_ticket(
    df_chat_final: pd.DataFrame,
    df_ticket_final: pd.DataFrame
) -> pd.DataFrame:
    
    merged = pd.merge(
        df_chat_final,
        df_ticket_final,
        how='outer',
        left_on='evt_ticket_ticketNumber',
        right_on='n_ticket',
        suffixes=('_chat', '_ticket')
    )
    return merged
    
    #Abaixo logica para captura de ticket number baseado em chat_id

def get_chat_id_from_number(chat_number: str) -> Optional[str]:
    
    params = {
        "filters[0][property]": "number",
        "filters[0][operator]": "eq",
        "filters[0][value]":    chat_number,
        "limit": 1
    }
    try:
        resp = requests.get(f"{octa_base_url}/chat",
                            headers=octa_headers,
                            params=params)
        resp.raise_for_status()
        data = resp.json()
        chats = data.get("results", data) if isinstance(data, dict) else data
        if chats:
            return chats[0].get("id")
    except requests.RequestException as e:
        print()
    return None

def get_ticket_number_from_chat_id(chat_id: str) -> Optional[int]:
    try:
        resp = requests.get(f"{octa_base_url}/chat/{chat_id}/events",
                            headers=octa_headers)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("results", data) if isinstance(data, dict) else data

        for ev in events:
            if ev.get("type") == "ticket":
                ticket_num = ev.get("data", {}).get("ticketNumber")
                if ticket_num is not None:
                    try:
                        return int(ticket_num)
                    except ValueError:
                        return ticket_num
    except requests.RequestException as e:
        print()
    return None


def get_ticket_number_from_chat_number(chat_number: str) -> Optional[int]:
   
    chat_id = get_chat_id_from_number(chat_number)
    if not chat_id:
        return None
    return get_ticket_number_from_chat_id(chat_id)


def find_ticket(df_conversas: pd.DataFrame) -> pd.DataFrame:
    
    ticket_series = df_conversas['number'].apply(get_ticket_number_from_chat_number)
    return pd.DataFrame({
        'number':        df_conversas['number'],
        'ticket_number': ticket_series
    })
# Busca completa do chat abaixo ( Utiliza dados de dataframes anteriores para conferencia e agrupamento)

def coleta_chat(
    df_numbers: pd.DataFrame,
    base_url: str,
    headers: Dict[str, str]
) -> pd.DataFrame:
    """
    Para cada número em df_numbers['number']:
      1) Obtém o ID interno do chat via GET /chat?filters[number]=eq
      2) Chama GET /chat/{chat_id} para detalhes do chat e dados de contato
      3) Chama GET /chat/{chat_id}/events para todos os eventos (ticket, satisfaction etc.)
      4) Normaliza e retorna um DataFrame com todas as colunas, inclusive todos os customFields de chat e de contato de forma dinâmica.
    """
    records: List[Dict[str, Any]] = []

    for num in df_numbers['number']:
        rec: Dict[str, Any] = {'number': num}

        try:
            # 1) Buscar ID interno do chat
            resp = requests.get(
                f"{base_url}/chat",
                headers=headers,
                params={
                    "filters[0][property]": "number",
                    "filters[0][operator]": "eq",
                    "filters[0][value]": str(num),
                    "limit": 1
                }
            )
            resp.raise_for_status()
            data = resp.json()

            # Extrair lista de resultados, tratando dict ou list
            if isinstance(data, dict):
                results = data.get("results", [])
            else:
                results = data if isinstance(data, list) else []

            if not results:
                rec['error'] = 'chat not found'
                records.append(rec)
                continue

            chat_id = results[0].get("id")
            rec['chat_id'] = chat_id

            # 2) Detalhes do chat e dados de contato
            resp_chat = requests.get(f"{base_url}/chat/{chat_id}", headers=headers)
            resp_chat.raise_for_status()
            chat_data = resp_chat.json()

            rec.update({
                "status": chat_data.get("status"),
                "created_at": chat_data.get("createdAt"),
                "closed_at": chat_data.get("ClosedAt"),
                "channel": chat_data.get("channel"),
                "department": chat_data.get("department"),
                "agent_name": chat_data.get("agent", {}).get("name"),
                "origin": chat_data.get("origin"),
                "Regiao": chat_data.get("Regiao"),
                "bairro": chat_data.get("bairro"),
                "satisfacao": chat_data.get("satisfacao")
            })

            for fld in chat_data.get("customFields", []):
                key = fld.get("key") or fld.get("name")
                rec[f"chat_cf_{key}"] = fld.get("value")

            contact = chat_data.get("contact", {})
            rec.update({
                "contact_id": contact.get("id"),
                "contact_name": contact.get("name"),
                "contact_email": contact.get("email"),
                "contact_phone": contact.get("phone")
            })
            for fld in contact.get("customFields", []):
                key = fld.get("key") or fld.get("name")
                rec[f"contact_cf_{key}"] = fld.get("value")

            resp_evt = requests.get(f"{base_url}/chat/{chat_id}/events", headers=headers)
            resp_evt.raise_for_status()
            evdata = resp_evt.json()
            evlist = evdata.get("results", []) if isinstance(evdata, dict) else (evdata if isinstance(evdata, list) else [])

            for ev in evlist:
                t = ev.get("type")
                data_ev = ev.get("data") or {}
                rec[f"evt_{t}"] = True
                if isinstance(data_ev, dict):
                    for k, v in data_ev.items():
                        rec[f"evt_{t}_{k}"] = v
                else:
                    rec[f"evt_{t}_raw"] = data_ev

        except requests.RequestException as e:
            rec['error'] = True
            rec['error_detail'] = str(e)

        records.append(rec)

    return pd.json_normalize(records)
