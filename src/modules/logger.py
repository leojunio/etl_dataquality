import logging
from logging.handlers import TimedRotatingFileHandler
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from .connections import Connections

# Carregar as variáveis do arquivo .env
load_dotenv()
PATH_LOGS = os.getenv("PATH_LOGS")
LOG_FILE = os.getenv("LOG_FILE_NAME", 'app.log')
RETENTION_TIME_LOGS = int(os.getenv("RETENTION_TIME_LOGS", 30))
TB_LOG_FULL_NAME = os.getenv("TB_LOG_FULL_NAME", 'TB_LOG_EXECUCAO')
SCHEMA = os.getenv("SCHEMA", 'STG')

class Logger:
    def __init__(self, path_logs=None, execution_id=None):
        self.path_logs = path_logs if path_logs else PATH_LOGS
        os.makedirs(self.path_logs, exist_ok=True)
        self.retention_time = RETENTION_TIME_LOGS
        self.log_name = LOG_FILE
        self.auto_logger = self.create_log_file()
        self.conn = Connections()
        self.table_log = TB_LOG_FULL_NAME
        self.schema = SCHEMA
        self.execution_id = '' if execution_id is None else str(execution_id)

    def create_log_file(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        log_filename = os.path.join(self.path_logs, self.log_name)
        file_handler = TimedRotatingFileHandler(
            log_filename, when="D", interval=1, backupCount=self.retention_time, encoding='utf-8'
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        return logger

    def set_permissions(self, directory):
        try:
            os.chmod(directory, 0o700)
            print(f"Permissões alteradas para {directory}")
        except OSError as e:
            print(f"Erro ao definir permissões: {e}")

    def info(self, message):
        self.auto_logger.info(message)

    def warning(self, message):
        self.auto_logger.warning(message)

    def error(self, message):
        self.auto_logger.error(message)
        #self.save_log_to_db(message, level='ERROR')

    def close_file(self):
        handlers = self.auto_logger.handlers[:]
        for handler in handlers:
            handler.close()
            self.auto_logger.removeHandler(handler)
        logging.shutdown()

    def save_log_to_db(self, message, level):
        connection = self.conn.connect_to_database('MSSQL')

        if connection:
            try:
                query = f"INSERT INTO {self.schema}.{self.table_log} (DS_LOG, TS_LOG, ID_EXECUCAO, TS_CARGA) VALUES (?, ?, ?, ?)"
                connection.execute(query, (message, datetime.now().strftime("%Y-%m-%d %H-%M-%S"), self.execution_id, datetime.now().strftime("%Y-%m-%d %H-%M-%S")))

            except Exception as e:
                print(f"Erro ao salvar log no banco de dados: {e}")
            finally:
                self.conn.close_connection(connection)
        else:
            print("Conexão com o banco de dados não está definida.")
