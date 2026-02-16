from pathlib import Path
import sys, os, time
from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import platform

# === Encontra o diretório src ===
THIS_FILE = Path(__file__).resolve()
SRC_DIR = THIS_FILE.parents[2]  # .../src
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Imports do projeto
from modules.logger import Logger
from modules.datafilehandler import DataFileReader
from modules.database import DatabaseHandler
from modules.util import converter_tempo
from modules.minio import MinIo
from pipeline.bronze.acesso_basico import AcessoBasico

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
TABLE_LOG_CONTROLE = os.getenv("DATABASE_TABLE_LOG_CONTROLE",'log.TB_LOGS_CARGA')
FILE_DIR = os.getenv("FILE_DIR")
BUCKET= os.getenv("S3_BUCKET")
PATH_LOGS_DBT = os.getenv("PATH_LOGS_DBT")
REMOVE_SOURCE_FILE = os.getenv("REMOVE_SOURCE_FILE", "true").lower()

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

    """ 
    if REMOVE_SOURCE_FILE:
        logger.info("Removendo arquivos da pasta de carga...")
        file.remove_arquivos(f"{FILE_DIR}")
     """
    # Obtém o ID da execução do DB_LOGS_CARGA
    execution_id = database.get_id_execution(PROJECT, TABLE_LOG_CONTROLE)

    # Executa pipeline de cobertura vacinal
    pipeline = AcessoBasico(execution_id, logger)
    pipeline.run()
    logger.info("Finalizado Landing Acesso Básico.")

    # Métrica de duração
    elapsed_time = time.time() - start_time
    final_time = converter_tempo(elapsed_time)
    logger.info(final_time)
    print(final_time)    
 
if __name__ == '__main__':
    logger = Logger()
    main(logger)
