from config import SRC_TABLE_SAC_OCTADESK, BQ
from ticket import update_ticket_status_by_ticket_id

sql = f"""
SELECT DISTINCT n_ticket
FROM {SRC_TABLE_SAC_OCTADESK}
WHERE (n_ticket is not null) AND (status_ticket != 'Resolvido')
"""

df_tabela = BQ.query(sql).to_dataframe()
tickets_list = df_tabela["n_ticket"].tolist()

for ticket in tickets_list:
    print(update_ticket_status_by_ticket_id(ticket))
