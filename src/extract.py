# Fazer a extração dos dados do IBGE
import requests
import logging
from pathlib import Path 
import json

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

logger = logging.getLogger(__name__)


ESTADOS = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
           "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
           "RO","RR","RS","SC","SE","SP","TO"]


def extrair_municipios_por_estado(sigla_uf:str) -> list:
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{sigla_uf}/municipios"
    logging.info("Iniciando extração")
    response= requests.get(url)
    if response.status_code == 200:
        dados = response.json()
        logging.info("API obtida com sucesso")
        logging.info(f"API funcionando e {len(dados)} registros encontrados")
        return dados
    else:
        raise Exception(f"Erro inesperado na API, status {response.status_code}")
    

def extrair_todos_municipios() -> list:
    lista_mun_completa = []
    for e in ESTADOS:
        lista_mun_completa.extend(extrair_municipios_por_estado(e))
    return lista_mun_completa

if __name__ == "__main__":
    resultado = extrair_todos_municipios()
    pasta = Path(__file__).parent.parent / "data" / "raw"
    pasta.mkdir(exist_ok=True, parents=True)
    arquivo = pasta / "municipios_brasil.json"
    arquivo.write_text(
        json.dumps(resultado, 
                   indent=4, 
                   ensure_ascii=False),
                   encoding="utf-8"
    )
    logging.info(f"{len(resultado)} Municípios extraídos")