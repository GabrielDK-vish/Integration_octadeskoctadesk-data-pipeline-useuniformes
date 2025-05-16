# Octadesk Data Pipeline - Use Uniformes SP

Este projeto implementa uma pipeline automatizada de coleta, transformação e carregamento de dados (ETL) a partir da plataforma **Octadesk**, com destino ao **Google BigQuery**, para uso analítico e organizacional pela **Use Uniformes SP**, após subir os registros com exceção de duplicadas ele faz atualização verificando o andamento de processos de tickets e atualizando-os diretamente no GCP.

## 🚀 Visão Geral

A solução é executada em uma máquina virtual com **Apache Airflow**, e realiza diariamente:

- Coleta paginada de **tickets** e **chats** da API da Octadesk
- Enriquecimento com campos personalizados (`customFields`) e eventos (ex.: criação de ticket, satisfação, encerramento)
- Integração entre tickets e conversas
- Upload dos dados em uma tabela do **Google BigQuery**, mantendo histórico completo

## 🧱 Estrutura do Projeto

```
.
├── main.py             # Script principal de execução
├── ticket.py           # Coleta e estruturação de tickets
├── chat.py             # Coleta, enriquecimento e normalização de conversas
├── config.py           # Carrega variáveis do .env
├── config.json         # Credenciais da conta de serviço GCP (não versionado)
├── .env                # Chaves de API da Octadesk (não versionado)
├── manutencao.py       # Verifica duplicidade de registro acessando tabela de destino.
└── requirements.txt    # Dependências do projeto
```

## 🗃️ Destino dos Dados

- **BigQuery Dataset**: `integracoes-infinit.DataLake_2025`
- **Tabela final**: `Sac_Octadesk`
- Os dados são normalizados, e uma coluna `upload` indica o horário da execução

## 🔐 Segurança

- Arquivos `.env` e `config.json` estão listados no `.gitignore`
- Variáveis de ambiente são carregadas com `python-dotenv`
- Autenticação com o BigQuery é feita via chave de serviço GCP

## ⚙️ Requisitos

- Python 3.10+
- Instalar as dependências:

```bash
pip install -r requirements.txt
```

## 🧪 Execução

```bash
python main.py
```

> A execução está automatizada via Airflow na VM da Use Uniformes SP.

## 📈 Utilidade

A centralização desses dados permite que a Use Uniformes SP:

- Analise atendimentos por canal, agente ou cliente
- Meça taxas de satisfação e performance por ticket
- Faça auditoria e visualizações em ferramentas de BI

---

**Desenvolvido com foco em confiabilidade, rastreabilidade e integração com o ecossistema GCP.**

## 📬 Contato

Desenvolvido por [Seu Nome].

- Email: ggbriel2k22@gmail.com
- LinkedIn: [Gabriel Ramos](https://www.linkedin.com/in/gabriel-ramos-401786356)

