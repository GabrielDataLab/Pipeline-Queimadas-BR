import boto3
import requests
from dotenv import load_dotenv
import json
import pandas as pd
import io
import os
from datetime import datetime
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")



s3_client = boto3.client(
    "s3",
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    region_name = "us-east-1"
)


def ler_do_s3(caminho_raw) -> list:
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    response = s3_client.get_object(Bucket=bucket_name, Key=caminho_raw)
    return json.loads(response["Body"].read())

def extrair_populacao() -> pd.DataFrame:
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2021/variaveis/9324?localidades=N6[all]"
    response = requests.get(url)
    if response.status_code == 200:
        dados_json = response.json()
        series = dados_json[0]["resultados"][0]["series"]
        df = pd.json_normalize(series)
        df = df[["localidade.id", "serie.2021"]].rename(columns={
            "localidade.id": "municipio_id",
            "serie.2021": "populacao"
        })
        return df
    raise Exception(f"Erro na API, status: {response.status_code}")

def transformar(dados_raw):
    df = pd.json_normalize(dados_raw)
    mapping = {
        'id': 'municipio_id',
        'nome': 'municipio_nome',
        'microrregiao.nome': 'microrregiao',
        'microrregiao.mesorregiao.nome': 'mesorregiao',
        'microrregiao.mesorregiao.UF.sigla': 'uf_sigla',
        'microrregiao.mesorregiao.UF.nome': 'uf_nome',
        'microrregiao.mesorregiao.UF.regiao.nome': 'regiao'
    }
    return df[list(mapping.keys())].rename(columns=mapping)

def salvar_processed_s3(df:pd.DataFrame, caminho_processed):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3_client.put_object(
        Bucket=os.getenv("AWS_BUCKET_NAME"), 
        Key=caminho_processed,
        Body=buffer.getvalue()
        )
    
if __name__ == "__main__":
    agora = datetime.now()
    caminho_raw = f"raw/municipios/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/municipios_brasil.json"
    caminho_processed = f"processed/municipios/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/municipios_brasil.parquet"
    
    dados_raw = ler_do_s3(caminho_raw)
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
    
    salvar_processed_s3(df_merged, caminho_processed)
    logging.info(f"Upload realizado com sucesso em: {caminho_processed} ")
