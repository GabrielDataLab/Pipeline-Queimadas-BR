import boto3
import os
import io
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import logging
import pandas as pd

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"
)

def criar_tabela(conn):
    sql_schema = "CREATE SCHEMA IF NOT EXISTS analytics;"
    
    sql_table = """
        CREATE TABLE IF NOT EXISTS analytics.municipios (
    municipio_id    VARCHAR(10) PRIMARY KEY,
    municipio_nome  VARCHAR(255),
    microrregiao    VARCHAR(255),
    mesorregiao     VARCHAR(255),
    uf_sigla        VARCHAR(2),
    uf_nome         VARCHAR(100),
    regiao          VARCHAR(50),
    populacao       BIGINT,
    porte           VARCHAR(10)
    );
        """
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql_schema)
            cur.execute(sql_table)
        conn.commit()
        logging.info("Schema: analytics e Tabela 'municipios' criada com sucesso")
    except Exception as e:
        logging.error(f"Erro ao criar estrutura: {e}")
        raise

def ler_parquet_s3(caminho_s3) -> pd.DataFrame:
    response = s3_client.get_object(Bucket=os.getenv("AWS_BUCKET_NAME"), Key=caminho_s3)
    buffer = io.BytesIO(response["Body"].read())
    df = pd.read_parquet(buffer)
    return df

def upsert(df:pd.DataFrame, conn):
    sql = """
        INSERT INTO analytics.municipios (municipio_id, municipio_nome, microrregiao, mesorregiao, 
                            uf_sigla, uf_nome, regiao, populacao, porte)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (municipio_id) 
    DO UPDATE SET
        municipio_nome = EXCLUDED.municipio_nome,
        populacao      = EXCLUDED.populacao,
        porte          = EXCLUDED.porte;
        """

    try:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False):
                cur.execute(sql,row) 
        conn.commit()
        logging.info(f"Upsert realizado com sucesso, {len(df)} registros concluidos")
    except Exception as e:
        logging.error(f"Erro ao fazer upsert dos dados: {e}")
        raise

if __name__ == "__main__":
    agora = datetime.now()
    caminho_s3 = f"processed/municipios/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/municipios_brasil.parquet"

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


    criar_tabela(conn)
    df = ler_parquet_s3(caminho_s3)
    upsert(df, conn)
    conn.close()
    logging.info("Carga finalizada com sucesso")
