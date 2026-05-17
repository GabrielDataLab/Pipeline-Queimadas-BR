import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import pendulum
from airflow import DAG
from airflow.decorators import task
from src.extract import extrair_todos_municipios, upload_para_s3
from src.transform import ler_do_s3, extrair_populacao, transformar, salvar_processed_s3
from src.load import ler_parquet_s3, criar_tabela, upsert
from pathlib import Path
import json
import pandas as pd
from datetime import datetime
import psycopg2



with DAG(
    dag_id="Pipeline_Municipios_IBGE",
    description="Pipeline ETL de municípios brasileiros via API IBGE",
    schedule=None,
    start_date=pendulum.datetime(2026,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ibge", "etl", "municipios"],
    default_args={
        "retries": 1

    }
) as dag:
    @task
    def extracao():
        resultado = extrair_todos_municipios()
        pasta = Path(__file__).parent.parent / "data" / "raw"
        pasta.mkdir(exist_ok=True, parents=True)
        arquivo = pasta / "municipios_brasil.json"
        arquivo.write_text(
            json.dumps(
            resultado,
            indent=4,
            ensure_ascii=False,), 
            encoding="utf-8")
        caminho_s3 = upload_para_s3(str(arquivo))
        return caminho_s3
    @task
    def transformacao(caminho_s3):
        agora = datetime.now()
        caminho_processed = f"processed/municipios/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/municipios_brasil.parquet"
        dados_raw = ler_do_s3(caminho_s3)
        dados_pop = extrair_populacao()
        dados_base = transformar(dados_raw)
        dados_base["municipio_id"] = dados_base["municipio_id"].astype(str)
        dados_pop["municipio_id"] = dados_pop["municipio_id"].astype(str)

        df_merged = dados_base.merge(dados_pop, how="inner", on="municipio_id")

        df_merged["populacao"] = pd.to_numeric(df_merged["populacao"], errors="coerce")
        df_merged = df_merged.dropna(subset=["populacao"])

        bins = [0, 20000, 100000, float("inf")]
        labels = ["pequeno", "medio", "grande"]

        df_merged["porte"] = pd.cut(df_merged["populacao"], bins=bins, labels=labels, include_lowest=True)
        salvar_processed_s3(df_merged,caminho_processed)
        return caminho_processed

    @task
    def carregar(caminho_processed):
        conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
        criar_tabela(conn)
        df = ler_parquet_s3(caminho_processed)
        upsert(df, conn)
        conn.close()


    caminho_s3 = extracao()
    caminho_processed = transformacao(caminho_s3)
    carregar(caminho_processed)
        
