import time, os, pytz
from modules.connections import Connections
from modules.util import converter_tempo
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
from pathlib import Path
import pandas as pd
from sqlalchemy import MetaData, Table, Column, text
from sqlalchemy.orm import sessionmaker

CWD=Path(os.path.realpath(__file__)).parent.parent

class DatabaseHandler:
    def __init__(self, logger=None):
        load_dotenv()

        self.logger = logger
        self.conn = Connections(logger=logger)     
        self.projeto = os.getenv("PROJECT")    
        self.database = os.getenv("DATABASE_TGT_HOST")
        self.batch_import_size = int(os.getenv("BATCH_IMPORT_SIZE",1000))
        self.schema = os.getenv("DATABASE_SCHEMA_TGT") 
        self.time = datetime.now().strftime("%Y-%m-%d %H-%M-%S") 
        self.table_log_full_name = os.getenv("TABLE_LOG_CONTROLE", 'log.TB_LOGS_CARGA')
        self.mssql_engine = self.conn.connect_to_database('MSSQL')
        self.timezone = pytz.timezone('America/Sao_Paulo')
        self.current_time = datetime.now(self.timezone)     
        #self.sql_path = os.path.join(CWD, 'sql', 'oracle_tables.sql')           

    def _count_table(self, table_name, schema=None):
        connection = self.conn.connect_to_database()

        try:
            query=f"SELECT COUNT(*) AS QTD_REGISTROS FROM {schema}.{table_name}"
            result = connection.execute(query)
            count = int(result.fetchone()[0])
            self.logger.info(f"Tabela {schema}.{table_name} lida com sucesso. Total de Registros: {count}")    
            print(f"Tabela {schema}.{table_name} lida com sucesso. Total de Registros: {count}")         
            return count
        
        except Exception as e:
            self.logger.error(f"Erro ao ler tabela {table_name}: {e}")

        #Fechando a Conexão    
        self.conn.close_connection(connection)


    def get_id_execution(self, project, table_log_full_name=None):
        connection = self.conn.connect_to_database('MSSQL')

        if table_log_full_name is None:
            table_log_full_name = self.table_log_full_name

        try:
            query = f"SELECT  ISNULL(MAX(ID_EXECUCAO),0) +1 AS ID_EXECUCAO FROM {table_log_full_name} WHERE NM_PROJETO = '{project}'"
            data = pd.read_sql(query, connection)
            id_execucao = data['ID_EXECUCAO'][0] if not data.empty else 1
            self.logger.info(f"ID da Execução: {id_execucao}")
            print(f"ID da Execução: {id_execucao}")
            return id_execucao
        
        except Exception as e:
            print(f"Ocorreu um erro: {str(e)}")
            self.logger.error(f"Ocorreu um erro: {str(e)}")
            return None        


    def create_dataframe_log(self, data, log_filename, execution_id):
        df = pd.DataFrame(data, columns=['DS_LOG'])
        df['TS_LOG'] = df['DS_LOG'].str.split(' - ').str[0]
        df['NM_FILE'] = os.path.basename(log_filename)
        df['ID_EXECUCAO'] = execution_id
        df['TS_CARGA'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df

    def load_summary(self, execution_id, nm_task, package, database_src, schema_src, table_src, database_tgt, schema_tgt, table_tgt, count_rows):

        # Dados para inserção
        project = self.projeto
        tracker_id = str(execution_id)
        table_count = str(count_rows)

        # SQL para inserção
        sql_insert = f"""
        INSERT INTO {self.table_log_full_name} (
            ID_EXECUCAO, NM_PROJETO, NM_PACKAGE, NM_TASK, NM_DATABASE_ORIGEM, NM_SCHEMA_ORIGEM, NM_TABELA_ORIGEM,
            NM_DATABASE_DESTINO, NM_SCHEMA_DESTINO, NM_TABELA_DESTINO, QT_REGISTROS, TS_CARGA
        ) VALUES (:tracker_id, :project, :package, :nm_task, :database_src, :schema_src, :table_src,
                :database_tgt, :schema_tgt, :table_tgt, :table_count, GETDATE())
        """

        # Dados a serem inseridos
        data_to_insert = {
            'tracker_id': tracker_id,
            'project': project,
            'package': package,
            'nm_task': nm_task,
            'database_src': database_src,
            'schema_src': schema_src,
            'table_src': table_src,
            'database_tgt': database_tgt,
            'schema_tgt': schema_tgt,
            'table_tgt': table_tgt,
            'table_count': table_count
        }

        try:
            #print(f"Dados que serão inseridos: {data_to_insert}")
            self.mssql_engine.execute(text(sql_insert), data_to_insert)
            self.mssql_engine.commit()

            print(f"Resumo dos dados carregados salvos com sucesso na tabela {self.table_log_full_name}. REGISTROS: {table_count}")
            self.logger.info(f"Resumo dos dados carregados salvos com sucesso na tabela {self.table_log_full_name}. REGISTROS: {table_count}")

        except Exception as e:
            self.logger.error(f"Erro ao salvar dados na tabela {self.table_log_full_name}: {e}")
            print(f"Erro ao salvar dados na tabela {self.table_log_full_name}: {e}")


    def schema_exists(self, user_name):
        connection = self.conn.connect_to_database('MSSQL')  
        with connection:
            result = connection.execute(
                text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'stg' and table_name like'{user_name}%'")
            )
            return result.scalar() is not None

    def drop_all_tables_in_schema(self, user_name):
        connection = self.conn.connect_to_database('MSSQL')  
        with connection:
            result = connection.execute(
                text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'stg' and table_name = '{user_name}'%")
            )
            tables = result.fetchall()
            for table in tables:
                table_name = table[0]
                connection.execute(text(f"DROP TABLE stg.{table_name}"))
                print(f"Tabela {table_name} dropada com sucesso.")
                self.logger.info(f"Tabela {table_name} dropada com sucesso.")


    def tables_exist_in_schema(self, user_name):
        connection = self.conn.connect_to_database('MSSQL') 
        with connection:
            result = connection.execute(
                text(f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = 'stg' AND table_name like'{user_name}%'")
            )
            return result.scalar() > 0


    def count_tables_in_schema(self, user_name):
        connection = self.conn.connect_to_database('MSSQL') 
        with connection:
            result = connection.execute(
                text(f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA = 'stg' AND table_name like '{user_name}%'")
            )
            return result.scalar()
    

    def save_df_to_table(self, df, table_name, schema):
        """"
        Recebe como parametro o dataframe df, a tabela e o schema do banco de dados:
        """
        
        if not self.mssql_engine:
            print(f"Falha ao conectar ao banco de dados MSSQL. Os dados não foram salvos na tabela {schema}.{table_name}")
            self.logger.error("Falha ao conectar ao banco de dados MSSQL.")
            return

        total_rows = len(df)

        if total_rows < 1:
            return
        
        if total_rows > 1:               
            start_time = time.time()
            transaction = self.mssql_engine.begin()

            try:
                df.to_sql(table_name, con=self.mssql_engine, schema=schema, if_exists='append', index=False)
                transaction.commit()     
                end_time = time.time()  # Marca o tempo final
                total_time = end_time - start_time  # Calcula o tempo da requisição      
                total_time = converter_tempo(total_time)

                if total_rows > 1:
                    self.logger.info(f"Salvando {total_rows} registros na tabela {schema}.{table_name} com sucesso em {total_time}")
                    print(f"Salvando {total_rows} registros na tabela {schema}.{table_name} com sucesso em {total_time}")                   
            except Exception as e:
                self.logger.error(f"Erro ao inserir dados no banco de dados: {e}")
                print(f"Erro ao inserir dados no banco de dados: {e}")                
                transaction.rollback()

        # Fecha a conexão
        #self.conn.close_connection(self.mssql_engine)


    def truncate_table(self, table_name: str, schema: str) -> bool:
        """
        Trunca a tabela [schema].[table_name] se existir.
        Retorna True se truncou, False se não existe ou houve falha.
        """

        # Helper de log com fallback (warning -> warn -> info -> print)
        def _log(level: str, msg: str):
            lg = getattr(self, "logger", None)
            if lg is None:
                print(msg); return
            if hasattr(lg, level):
                getattr(lg, level)(msg); return
            if level == "warning" and hasattr(lg, "warn"):
                lg.warn(msg); return
            if hasattr(lg, "log"):
                lvlno = getattr(logging, level.upper(), logging.INFO)
                lg.log(lvlno, msg); return
            if hasattr(lg, "info"):
                lg.info(msg); return
            print(msg)

        if not self.mssql_engine:
            msg = f"Falha ao conectar ao banco de dados MSSQL. Truncate da tabela {schema}.{table_name} não executado"
            print(msg)
            _log("error", msg)
            return False

        Session = sessionmaker(bind=self.mssql_engine)
        session = Session()

        try:
            # Verifica se a tabela existe (sys.schemas + sys.tables é robusto no SQL Server)
            exists = session.execute(
                text("""
                    SELECT 1
                    FROM sys.tables t
                    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE s.name = :schema AND t.name = :table
                """),
                {"schema": schema, "table": table_name}
            ).scalar()

            if not exists:
                msg = f"Não há tabela para truncar: {schema}.{table_name}"
                print(msg)
                _log("warning", msg)  # usa fallback se .warning não existir
                return False

            # Nome qualificado com escape de colchetes para T-SQL
            esc_schema = schema.replace("]", "]]")
            esc_table  = table_name.replace("]", "]]")
            qualified  = f"[{esc_schema}].[{esc_table}]"

            _log("info", f"Limpando a tabela {qualified}")
            print(f"Limpando a tabela {qualified}")

            session.execute(text(f"TRUNCATE TABLE {qualified}"))
            session.commit()

            msg = f"Tabela {qualified} truncada com sucesso."
            print(msg)
            _log("info", msg)
            return True

        except Exception as e:
            session.rollback()
            msg = f"Erro ao limpar tabela {schema}.{table_name}: {e}"
            print(msg)
            _log("error", msg)
            return False

        finally:
            session.close()