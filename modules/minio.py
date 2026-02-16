import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

class MinIo:
    def __init__(self, logger=None):
        load_dotenv()

        self.minio_endpoint = os.getenv("S3_ENDPOINT_URL")
        self.region = os.getenv("S3_REGION")
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")        
        self.s3_client = self.connect_to_minio()
        self.msg_error_connection = "Erro: Cliente MinIO não está conectado."
        self.logger = logger 

    def connect_to_minio(self):
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=self.minio_endpoint,
                region_name= self.region,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key
            )
            return s3_client
        except Exception as e:
            print(f"Erro ao conectar ao MinIO: {e}")
            return None

    def list_buckets_minio(self):
        if self.s3_client is None:
            print(self.msg_error_connection)
            self.logger.info(self.msg_error_connection)
            return

        try:
            response = self.s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
            if buckets:
                print("Buckets existentes:")
                for bucket in buckets:
                    print(bucket)
            else:
                print("Não existem buckets!")
        except Exception as e:
            print(f"Erro ao listar os buckets: {e}")
            self.logger.error(f"Erro ao listar os buckets: {e}")

    def create_bucket(self, bucket_name):
        if self.s3_client is None:
            print(self.msg_error_connection)
            self.logger.error(self.msg_error_connection)
            return False
        
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            print(f"O bucket '{bucket_name}' já existe.")
            self.logger.info(f"O bucket '{bucket_name}' já existe.")
            return True
        
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.s3_client.create_bucket(Bucket=bucket_name)
                    print(f"Bucket '{bucket_name}' criado com sucesso.")
                    self.logger.info(f"Bucket '{bucket_name}' criado com sucesso.")
                    return True
                except ClientError as ce:
                    print(f"Erro ao criar o bucket '{bucket_name}': {ce}")
                    self.logger.info(f"Erro ao criar o bucket '{bucket_name}': {ce}")
            else:
                print(f"Erro ao verificar o bucket '{bucket_name}': {e}")
                self.logger.error(f"Erro ao verificar o bucket '{bucket_name}': {e}")
            return False

    def delete_bucket(self, bucket_name):
        if self.s3_client is None:
            print(self.msg_error_connection)
            self.logger.error(self.msg_error_connection)
            return False
        
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"O bucket '{bucket_name}' não foi encontrado.")
                self.logger.error(f"O bucket '{bucket_name}' não foi encontrado.")
                return False

        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"Objetos dentro do bucket '{bucket_name}' foram removidos.")
                self.logger.info(f"Objetos dentro do bucket '{bucket_name}' foram removidos.")

            self.s3_client.delete_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' foi apagado com sucesso.")
            self.logger.info(f"Bucket '{bucket_name}' foi apagado com sucesso.")
            return True
        except ClientError as e:
            print(f"Erro ao apagar o bucket '{bucket_name}': {e}")
            self.logger.error(f"Erro ao apagar o bucket '{bucket_name}': {e}")
            return False

    def upload_to_minio(self, file_path, bucket_name, file_type):
        if self.s3_client is None:
            print(self.msg_error_connection)
            self.logger.error(self.msg_error_connection)
            return False

        if not self.create_bucket(bucket_name):
            return

        try:
            # Verifica se existem arquivos com a extensão informada
            files = [file for file in os.listdir(file_path) if file.endswith(f".{file_type}")]

            if not files:
                print(f"Não existem arquivos .{file_type} na pasta {file_path}.")
                self.logger.info(f"Não existem arquivos .{file_type} na pasta {file_path}.")
                return

            # Upload de cada arquivo para o MinIO
            for file in files:
                full_file_path = os.path.join(file_path, file)
                print(f"Enviando o arquivo {file} para o bucket {bucket_name}.")
                self.logger.info(f"Enviando o arquivo {file} para o bucket {bucket_name}.")
                
                self.s3_client.upload_file(full_file_path, bucket_name, file_type + '/' + file)
                print(f"Arquivo {file} enviado com sucesso para o bucket {bucket_name}.")
                self.logger.info(f"Arquivo {file} enviado com sucesso para o bucket {bucket_name}.")

        except Exception as e:
            print(f"Erro ao enviar o arquivo para o {bucket_name}: {e}")
            self.logger.error(f"Erro ao enviar o arquivo para o {bucket_name}: {e}")