# Fazer o load dos dados em DB postgres
import psycopg2
import boto3
import pandas as pd
import logging
import io
import os
from datetime import datetime
from dotenv import load_dotenv

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
    sql = """
        CREATE TABLE IF NOT EXISTS municipios (
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
            cur.execute(sql)
        conn.commit()
        logging.info("Tabela 'municipios' criada com sucesso")
    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao criar a tabela: {e}")
        raise


def ler_parquet_s3(caminho_s3: str) -> pd.DataFrame:
    response = s3_client.get_object(Bucket=os.getenv("AWS_BUCKET_NAME"), Key=caminho_s3)
    buffer = io.BytesIO(response["Body"].read())
    df = pd.read_parquet(buffer)
    return df

def upsert(df:pd.DataFrame, conn):
    sql= """
    INSERT INTO municipios (municipio_id, municipio_nome, microrregiao, mesorregiao, 
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
        logging.info(f"Upsert de {len(df)} registros concluídos.")
    except Exception as e:
            conn.rollback()
            logging.error(f"Erro ao executar upsert: {e}")
            raise


if __name__ == "__main__":
    agora = datetime.now()
    caminho_s3 = f"processed/municipios/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/municipios_brasil.parquet"

    conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT")
    )

    criar_tabela(conn)
    df = ler_parquet_s3(caminho_s3)
    upsert(df, conn)
    conn.close()
    logging.info("Carga finalizada com sucesso")
    