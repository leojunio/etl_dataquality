from __future__ import annotations
from pathlib import Path
from datetime import datetime
import os, sys, json
from dotenv import load_dotenv
import pandas as pd
import pandera.pandas as pa
import pytz

# === Encontra o diretório src ===
THIS_FILE = Path(__file__).resolve()
SRC_DIR = THIS_FILE.parents[2]  # .../src
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipeline._contracts.acesso_basico_contract import AcessoBasicoContract as abc
from modules.datafilehandler import DataFileReader
from modules.database import DatabaseHandler
from modules.util import converter_tempo
from modules.logger import Logger
from modules.transform import to_float_ptbr as tf

class AcessoBasico:
    def __init__(self, id_execucao: int, logger: logger):
        load_dotenv()

        self.logger = logger
        self.reader = DataFileReader(logger=logger)
        self.database = DatabaseHandler(logger=logger)

        self.logger = logger
        self.database = DatabaseHandler(logger=logger)
        self.reader = DataFileReader(logger=logger)

        self.timezone = pytz.timezone("America/Sao_Paulo")
        self.ts_now = pd.Timestamp.now(self.timezone)
        self.id_execucao = id_execucao

        # Dados do Banco de Dados
        self.database_name_tgt = os.getenv("DATABASE_NAME_TGT")
        self.table_schema_tgt = os.getenv("DATABASE_SCHEMA_TGT")
        self.table_name_tgt = os.getenv("DATABASE_TABLE_TGT")

        if not self.table_name_tgt:
            raise ValueError("Variável de ambiente DATABASE_TABLE_TGT não definida.")
        if not self.table_schema_tgt:
            raise ValueError("Variável de ambiente DATABASE_SCHEMA_TGT não definida.")
        if not self.database_name_tgt:
            # não é usada diretamente aqui, mas já validamos
            self.logger.warning("DATABASE_NAME_TGT não definida. Verifique o .env se for necessária.")

        self.package_name = os.getenv("PROJECT", "NAO_INFORMADO")
        self.task_name = f"IMPORT_{self.table_name_tgt}"

        # Pastas/arquivos
        #self.remove_source_file = os.getenv("REMOVE_SOURCE_FILE", "false").lower() == "true"
        self.files_dir = (SRC_DIR / "pipeline" / "landing" / "in")
        self.schema_path = (SRC_DIR / "pipeline" / "landing" / "schemas" / "csv" / "acesso_basico.json")


    def extract(self) -> pd.DataFrame:
        # Carrega parâmetros do schema (aproveitamos delimiter/encoding)
        schema_map, ordered_names, delimiter, encoding = self.reader.load_generic_schema(self.schema_path)

        # Padrões de arquivo 
        patterns = [
            "*acesso*basico*"
            #"*saude*bucal*",
        ]

        try:
            df = self.reader.read_folder(
                folder=self.files_dir,
                patterns=patterns,              
                schema_filename=self.schema_path, 
                delimiter=delimiter,
                validate_columns=True,
                normalize_names=True,             
                filename_column="NM_SOURCE_FILE", 
            )
        except FileNotFoundError as e:
            self.logger.warning(f"Nenhum arquivo encontrado: {e}")
            # Retorna DF vazio com as colunas do schema
            return pd.DataFrame(columns=ordered_names or [])

        self.logger.info(f"Extract OK | linhas={len(df)} | colunas={list(df.columns)}")

        return df

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["ID_EXECUCAO"] = self.id_execucao
        out["TS_CARGA"] = self.ts_now.isoformat()
        self.logger.info(f"Enrich OK | ID_EXECUCAO={self.id_execucao} | TS_CARGA={self.ts_now.isoformat()}")
        return out

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        - Remove linhas totalmente vazias.
        - Normaliza a coluna NM_SOURCE_FILE (se existir):
        remove o literal "[COMPLETO]" e aplica strip.
        Converte vazios de volta para NA (opcional).
        """
        if df is None:
            raise ValueError("df não pode ser None")

        before = len(df)
        out = df.dropna(how="all").copy()

        col = "NM_SOURCE_FILE"
        if col in out.columns:
            out[col] = (
                out[col].astype("string").fillna("").str.replace("[COMPLETO]", "", regex=False) .str.strip()).replace("", pd.NA)
        else:
            self.logger.warning(f"Coluna '{col}' não encontrada; pulando normalização.")

        self.logger.info(f"Transform OK | {before} -> {len(out)} linhas")

        float_cols = ["MEDIA_RELATIVA", "MEDIA_ABSOLUTA", "VALOR_RELATIVO", "VALOR_ABSOLUTO"]

        for c in float_cols:
            if c in out.columns:
                out[c] = tf(out[c])
        try:
            df_out = abc.validate(out, lazy=True)
            return df_out
        
        except pa.errors.SchemaErrors as e:
            self.logger.error(f"Erro de validação dos dados: {e}")
            raise

    def truncate(self) -> bool:
        return self.database.truncate_table(self.table_name_tgt, self.table_schema_tgt)

    def load(self, df: pd.DataFrame) -> None:
        self.database.save_df_to_table(df, self.table_name_tgt, self.table_schema_tgt)
        self.logger.info(f"Load OK | {len(df)} linhas em {self.table_schema_tgt}.{self.table_name_tgt}")

    def load_summary_log(self, df: pd.DataFrame) -> pd.DataFrame:

        out = (
            df.groupby("NM_SOURCE_FILE").size().reset_index(name="QT_REGISTROS")
        )

        for row in out.itertuples(index=False):
            self.database.load_summary(
                execution_id=self.id_execucao,
                package = self.package_name,
                nm_task = self.task_name,          
                database_src = "file",
                schema_src = "csv",
                table_src = row.NM_SOURCE_FILE,
                database_tgt = self.database_name_tgt,
                schema_tgt = self.table_schema_tgt,
                table_tgt = self.table_name_tgt,
                count_rows = int(row.QT_REGISTROS),
            )

        return out
    
    def run(self) -> pd.DataFrame:
        start = datetime.now(self.timezone)
        self.logger.info(f"Iniciando {self.task_name}")

        df = self.extract()
        df = self.enrich(df)
        df = self.transform(df)

        # se estiver vazio, paramos a execução e registra o log
        if df.empty:
            self.logger.warning("Pipeline sem dados após transform. Encerrando sem truncate/load.")
            return df

        self.truncate()
        self.load(df)
        self.load_summary_log(df)

        elapsed = datetime.now(self.timezone) - start
        final_time = converter_tempo(elapsed)        
        
        print(f"Finalizado {self.task_name} com a duração {final_time}")
        self.logger.info(f"Finalizado {self.task_name} com a duração {final_time}")
  
        return df

if __name__ == "__main__":
    PATH_LOGS = os.getenv("PATH_LOGS")
    logger = Logger(path_logs=PATH_LOGS)
    pipeline = AcessoBasico(id_execucao=0, logger=logger)
    pipeline.run()