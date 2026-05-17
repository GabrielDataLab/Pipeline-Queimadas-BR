# Pipeline IBGE-BR 🇧🇷

Pipeline ETL automatizado que coleta dados de todos os **5.570 municípios brasileiros** via API pública do IBGE, enriquece com dados populacionais do Censo 2021, e carrega em um banco PostgreSQL — orquestrado com Apache Airflow e containerizado com Docker.

---

## Arquitetura

```
┌─────────────────────┐     ┌─────────────────────┐
│   API IBGE v1       │     │   API IBGE v3        │
│  (Municípios)       │     │  (População 2021)    │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
         └──────────┬──────────────────┘
                    ▼
         ┌──────────────────┐
         │   Extract (Python)│
         │  extrair_todos_  │
         │   municipios()   │
         └────────┬─────────┘
                  │
                  ▼
     ┌────────────────────────┐
     │  AWS S3 — Camada RAW   │
     │  raw/municipios/       │
     │  ano=YYYY/mes=MM/      │
     │  dia=DD/               │
     │  municipios_brasil.json│
     └────────┬───────────────┘
              │
              ▼
     ┌────────────────────┐
     │  Transform (Python) │
     │  · Normalização     │
     │  · Merge população  │
     │  · Classificação    │
     │    de porte         │
     └────────┬───────────┘
              │
              ▼
     ┌──────────────────────────┐
     │  AWS S3 — Camada         │
     │  PROCESSED               │
     │  processed/municipios/   │
     │  ano=YYYY/mes=MM/dia=DD/ │
     │  municipios_brasil.      │
     │  parquet                 │
     └────────┬─────────────────┘
              │
              ▼
     ┌──────────────────────┐
     │   Load (psycopg2)    │
     │   Upsert → PostgreSQL│
     │   analytics.municipios     │
     └──────────────────────┘
              ▲
              │
     ┌────────────────┐
     │ Apache Airflow │
     │  (orquestração)│
     └────────────────┘
```

---

## Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 2.8.1 |
| Linguagem | Python 3.11 |
| Armazenamento | AWS S3 (raw + processed) |
| Banco de dados | PostgreSQL 15 |
| Containerização | Docker + Docker Compose |
| Formato colunar | Apache Parquet (PyArrow) |
| Integração AWS | Boto3 |

---

## Estrutura do Projeto

```
pipeline-ibge-br/
├── dags/
│   └── dag_municipios.py       # DAG principal — TaskFlow API
├── src/
│   ├── extract.py              # Coleta via API IBGE + upload S3
│   ├── transform.py            # Normalização, merge e classificação
│   └── load.py                 # Criação de tabela + upsert PostgreSQL
├── .env.example                # Template de variáveis de ambiente
├── docker-compose.yml          # Airflow + PostgreSQL
├── Dockerfile
└── requirements.txt
```

---

## Como Rodar Localmente

### Pré-requisitos

- Docker e Docker Compose instalados
- Conta AWS com bucket S3 criado
- Credenciais AWS com permissão de leitura/escrita no bucket

### 1. Clonar e configurar variáveis de ambiente

```bash
git clone https://github.com/GabrielDataLab/pipeline-ibge-br.git
cd pipeline-ibge-br
cp .env.example .env
```

Edite o arquivo `.env` com suas credenciais:

```env
AWS_ACCESS_KEY_ID=sua_chave
AWS_SECRET_ACCESS_KEY=sua_chave_secreta
AWS_BUCKET_NAME=nome-do-seu-bucket

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=ibge
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/ibge
```

### 2. Subir os containers

```bash
docker compose up -d
```

Aguarde o healthcheck do PostgreSQL passar (cerca de 20 segundos).

### 3. Executar o pipeline

Acesse o Airflow em `http://localhost:8080`, ative a DAG `Pipeline_Municipios_IBGE` e dispare manualmente.

O pipeline irá:
1. Coletar dados dos 27 estados via API IBGE
2. Fazer upload do JSON bruto para o S3
3. Enriquecer com dados populacionais do Censo 2021
4. Salvar o resultado em Parquet particionado no S3
5. Carregar na tabela `analytics.municipios` do PostgreSQL via upsert

---

## Resultado Final

Tabela `analytics.municipios` no PostgreSQL com **5.570 registros**, contendo:

| Coluna | Descrição |
|---|---|
| `municipio_id` | Código IBGE do município (PK) |
| `municipio_nome` | Nome do município |
| `microrregiao` | Microrregião IBGE |
| `mesorregiao` | Mesorregião IBGE |
| `uf_sigla` | Sigla do estado |
| `uf_nome` | Nome do estado |
| `regiao` | Região do Brasil |
| `populacao` | População estimada (Censo 2021) |
| `porte` | Classificação: `pequeno` / `medio` / `grande` |

Classificação de porte:
- **Pequeno:** até 20.000 habitantes
- **Médio:** de 20.001 a 100.000 habitantes
- **Grande:** acima de 100.000 habitantes

---

## Destaques de Engenharia

- **Idempotência:** upsert com `ON CONFLICT` garante que reexecuções não duplicam dados
- **Particionamento Hive-style no S3:** compatível com AWS Athena e Glue Crawler sem configuração adicional
- **Separação de camadas:** raw (JSON) e processed (Parquet) seguem o padrão de data lake em medallion architecture
- **Containerização completa:** ambiente 100% reproduzível via Docker Compose
- **Variáveis de ambiente:** nenhuma credencial hardcoded no código

---

## Autor

**Gabriel Medeiros** — [LinkedIn](https://linkedin.com/in/gabrielhdata) · [GitHub](https://github.com/GabrielDataLab)