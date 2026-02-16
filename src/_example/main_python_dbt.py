from pathlib import Path
import sys, os, time

# === Encontra o diretório src ===
THIS_FILE = Path(__file__).resolve()
SRC_DIR = THIS_FILE.parents  # .../src
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import platform

# Imports do projeto
from modules.logger import Logger
from modules.datafilehandler import DataFileReader
from modules.database import DatabaseHandler
from modules.util import converter_tempo
from modules.minio import MinIo
from pipeline.stg.stg_cobertura_vacinal import StgCoberturaVacinal

# Runner do dbt
from modules.dbt_runner import (
    run_dbt_run,
    run_dbt_test,
    ENV_FILE,
    DBT_DIR,  
)

# Carrega variáveis de ambiente da raiz
load_dotenv(dotenv_path=ENV_FILE, override=True)

# Constantes
PATH_LOGS = os.getenv("PATH_LOGS")
LOG_FILE = os.getenv("LOG_FILE")
PROJECT = os.getenv("PROJECT")
SCHEMA = os.getenv("SCHEMA")
TABLE_LOG_FULL_NAME = os.getenv("TB_LOG_FULL_NAME", 'TB_LOG_EXECUCAO')
TABLE_LOG_CONTROLE = os.getenv("TABLE_LOG_CONTROLE",'DB_LOG_CARGA.dbo.TB_LOGS_CARGA')
DOWNLOAD_DIR = os.getenv("APP_DOWNLOAD_DIR")
BUCKET= os.getenv("S3_BUCKET")
PATH_LOGS_DBT = os.getenv("PATH_LOGS_DBT")

if not PATH_LOGS_DBT:
    PATH_LOGS_DBT = (
        os.path.join(os.getcwd(), "pipeline", "logs")
        if platform.system().lower() == "windows"
        else "/pipeline/logs"
    )

def main(logger: Logger) -> None:
    start_time = time.time()
    minio = MinIo(logger)
    database = DatabaseHandler(logger)
    file = DataFileReader(logger)

    print('Início da Execução')

    # Remove arquivos da pasta de carga
    file.remove_arquivos(f"{DOWNLOAD_DIR}")
    
    # Baixa os arquivos do Sharepoint
    ds.main(logger)

    # Obtém o ID da execução do DB_LOGS_CARGA
    execution_id = database.get_id_execution(PROJECT, TABLE_LOG_CONTROLE)

    # Executa pipeline de cobertura vacinal
    pipeline = StgCoberturaVacinal(execution_id, logger)
    pipeline.run()
    logger.info("Finalizado stg_cobertura_vacinal.")

    print(f"\n Iniciando a Execução do DBT - WRK, DDM e ACC: Veja o log no caminho {PATH_LOGS_DBT}")
    logger.info(f"\n Iniciando a Execução do DBT - WRK, DDM e ACC: Veja o log no caminho {PATH_LOGS_DBT}")    

    #=== dbt: run → tests > docs generate ===
    run_dbt_run(logger)
    logger.info("dbt run concluído.") 

    run_dbt_test(logger)
    logger.info("dbt run concluído.") 

    #Enviando os arquivos de log para o Bucket
    minio.upload_to_minio(PATH_LOGS, BUCKET, "log")

    #Enviando os arquivos de log para o Bucket
    minio.upload_to_minio(PATH_LOGS_DBT, BUCKET, "log")

    # Métrica de duração
    elapsed_time = time.time() - start_time
    final_time = converter_tempo(elapsed_time)
    logger.info(final_time)
    print(final_time)    
 
if __name__ == '__main__':
    logger = Logger()
    main(logger)
