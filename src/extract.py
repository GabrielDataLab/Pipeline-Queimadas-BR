import requests
import logging
import json
from pathlib import Path
from datetime import datetime
import os
import boto3
from dotenv import load_dotenv

load_dotenv()


ESTADOS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
           "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
           "RO","RR","RS","SC","SE","SP","TO"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


def extrair_municipios_por_estado(sigla_uf:str) -> list:
    logging.info("Extração iniciada")
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{sigla_uf}/municipios"
    response = requests.get(url)
    if response.status_code == 200:
        dados = response.json()
        logging.info(f"Extração realizada com sucesso! {len(dados)} Registros encontrados")
        return dados
    else:
        raise Exception(f"Erro inesperado na API, status: {response.status_code}")
    

def upload_para_s3(caminho_arquivo_local:Path) -> str:
    
    agora = datetime.now()
    dia = agora.strftime("%d")
    mes = agora.strftime("%m")
    ano = agora.strftime("%Y")
    
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    caminho_s3 = f"raw/municipios/ano={ano}/mes={mes}/dia={dia}/municipios_brasil.json"
    s3.upload_file(
        str(caminho_arquivo_local),
        bucket_name,
        caminho_s3
    )
    logging.info(f"Upload realizado com sucesso!")
    return caminho_s3


def extrair_todos_municipios() -> list:
    lista_municipios_completa = []
    for e in ESTADOS:
        lista_municipios_completa.extend(extrair_municipios_por_estado(e))
    return lista_municipios_completa

if __name__ == "__main__":
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
    logging.info(f"Extração finalizada, {len(resultado)} Municipios extraídos")
    caminho_s3 = upload_para_s3(arquivo)
    logging.info(f"Arquivo disponivel em: {caminho_s3}")