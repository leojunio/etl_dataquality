from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import URL, create_engine, text
import warnings
from sqlalchemy import exc as sa_exc

import os
import pyodbc

# Definir o caminho do diretório atual
CWD = Path(os.path.realpath(__file__)).parent.parent

# silencia apenas o warning de versão do SQL Server
warnings.filterwarnings(
    "ignore",
    r"^Unrecognized server version info",
    sa_exc.SAWarning,
)

class Connections:
    
    def __init__(self, logger=None):
        load_dotenv()
        self.logger = logger
        self.pool_size = 10
        self.max_overflow_pool = 20

    def create_mssql_config(self):
        return {
            'username': os.getenv('DATABASE_TGT_USERNAME'),
            'password': os.getenv('DATABASE_TGT_PASSWORD'),
            'server': os.getenv('DATABASE_TGT_HOST'),
            'database': os.getenv('DATABASE_TGT_DBNAME'),
            'port': os.getenv('DATABASE_TGT_PORT'),
            'sgbd': 'MSSQL'
        }

    def create_orcl_config(self):
        return {
            'username': os.getenv("DATABASE_USER_NAME_SCR"),
            'password': os.getenv("DATABASE_USER_PASSWORD_SCR"),
            'server': os.getenv("DATABASE_HOST_SRC"),
            'port': os.getenv("DATABASE_PORT_SRC"),
            'service_name': os.getenv("DATABASE_SERVICE_NAME_SRC"),
            'sgbd': 'ORCL'
        }

    def connect_to_database(self, sgbd):
        try:
            db_config = self.get_db_config(sgbd)
            connection = self.connect(**db_config)
            
            if connection:
                return connection
            else:
                self.logger.error("Erro ao conectar ao banco de dados: Nenhuma conexão estabelecida.")
                return None
        except Exception as e:
            self.logger.error(f"Erro ao conectar ao banco de dados: {e}")
            return None

    def get_db_config(self, sgbd):
        if sgbd == 'MSSQL':
            return self.create_mssql_config()
        elif sgbd == 'ORCL':
            return self.create_orcl_config()
        else:
            raise ValueError("Tipo de banco de dados não suportado.")

    def connect(self, **config):
        sgbd = config.get('sgbd')
        
        if sgbd == "MSSQL":
            return self.connect_mssql(**config)
        elif sgbd == "ORCL":
            return self.connect_orcl(**config)
        else:
            self.logger.error(f"SGDB não suportado: {sgbd}")
            return None

    def connect_mssql(self, **config):
        """
        Conecta ao SQL Server usando o melhor driver disponível (18 ou 17).
        Espera chaves: username, password, server, port, database.
        Retorna uma Connection do SQLAlchemy (engine.connect()) ou None.
        """
        try:
            # 1) Escolhe o driver disponível no sistema (preferindo 18)
            preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
            available = {d.strip() for d in pyodbc.drivers()}
            driver = next((d for d in preferred if d in available), None)

            if not driver:
                self.logger.error(f"Nenhum driver ODBC SQL Server encontrado. Drivers instalados: {sorted(available)}")
                return None

            # 2) Monta a URL de conexão de forma segura (sem se preocupar com encoding)
            url = URL.create(
                "mssql+pyodbc",
                username=config["username"],
                password=config["password"],
                host=config["server"],
                port=int(config.get("port", 1433)),
                database=config["database"],
                query={
                    "driver": driver,            
                    "Encrypt": "yes",
                    "TrustServerCertificate": "yes",    # ajuste para "no" se usar CA válida
                },
            )

            # 3) Cria engine (com otimizações úteis)
            engine = create_engine(
                url,
                pool_size=getattr(self, "pool_size", 5),
                max_overflow=getattr(self, "max_overflow_pool", 10),
                pool_pre_ping=True,
                fast_executemany=True,   
            )

            return engine.connect()

        except Exception as e:
            self.logger.error(f"Erro ao conectar ao MSSQL: {e}")
            return None

    def connect_orcl(self, **config):
            try:
                conn_args={
                        "user": config['username'],
                        "password": config['password'],
                        "host": config['server'],
                        "port": config['port'],
                        "service_name": config['service_name']
                    }

                #self.logger.info(f"connect_args: {conn_args}")
                
                engine = create_engine(
                    f'oracle+oracledb://:@',
                    thick_mode=True,
                    connect_args=conn_args
                )                    

                return engine.connect()

            except oracledb.DatabaseError as e:
                self.logger.error(f"Erro ao conectar ao ORCL: {e}")
                return None

    def execute_query(self, query, connection):
        if connection:
            try:
                result = connection.execute(query)
                return result
            except Exception as e:
                self.logger.error(f"Erro ao executar a query: {e}")
            finally:
                self.close_connection(connection)
        return None

    def close_connection(self, connection):
        if connection:
            try:
                connection.close()
            except Exception as e:
                self.logger.error(f"Erro ao fechar conexão com o banco de dados: {e}")
