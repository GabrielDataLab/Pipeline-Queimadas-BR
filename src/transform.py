# Fazer a transformaçoes dos dados com pandas
import boto3
import pandas as pd
import json
import os
import io
from datetime import datetime
import requests
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

load_dotenv()

# Configuração de Cliente S3 Reutilizável
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"
)

def ler_do_s3(chave_s3: str) -> list:
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    response = s3_client.get_object(Bucket=bucket_name, Key=chave_s3)
    return json.loads(response["Body"].read())

def extrair_populacao() -> pd.DataFrame:
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2021/variaveis/9324?localidades=N6[all]"
    response = requests.get(url)
    if response.status_code == 200:
        dados_json = response.json()
        # Acessa a lista de séries dentro da estrutura do IBGE
        series = dados_json[0]["resultados"][0]["series"]
        df = pd.json_normalize(series)
        
        df = df[["localidade.id", "serie.2021"]].rename(columns={
            "localidade.id": "municipio_id",
            "serie.2021": "populacao"
        })
        return df
    raise Exception(f"Erro API IBGE: {response.status_code}")

def transformar(dados_raw: list) -> pd.DataFrame:
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

def salvar_processed_s3(df: pd.DataFrame, caminho_s3: str):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3_client.put_object(
        Bucket=os.getenv("AWS_BUCKET_NAME"),
        Key=caminho_s3,
        Body=buffer.getvalue()
    )

if __name__ == "__main__":
    agora = datetime.now()
    
    # 1. Caminhos Dinâmicos
    caminho_raw = f"raw/municipios/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/municipios_brasil.json"
    caminho_processed = f"processed/municipios/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/municipios_brasil.parquet"

    # 2. Extração
    logging.info("Iniciando extração...")
    dados_s3 = ler_do_s3(caminho_raw)
    df_pop = extrair_populacao()

    # 3. Transformação e Merge
    df_base = transformar(dados_s3)
    
    # Garantia de Tipagem para o Merge
    df_base["municipio_id"] = df_base["municipio_id"].astype(str)
    df_pop["municipio_id"] = df_pop["municipio_id"].astype(str)

    df_merged = df_base.merge(df_pop, on="municipio_id", how="inner")

    # 4. Limpeza e Categorização (Porte)
    df_merged["populacao"] = pd.to_numeric(df_merged["populacao"], errors="coerce")
    df_merged = df_merged.dropna(subset=["populacao"])
    
    bins = [0, 20000, 100000, float("inf")]
    labels = ["pequeno", "medio", "grande"]
    df_merged["porte"] = pd.cut(df_merged["populacao"], bins=bins, labels=labels, include_lowest=True)

    # 5. Carga (Load)
    salvar_processed_s3(df_merged, caminho_processed)
    logging.info(f"Transform concluído: {len(df_merged)} municípios processados, "
             f"{df_merged['porte'].value_counts().to_dict()}")