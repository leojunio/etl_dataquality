FROM python:3.11-slim-bullseye

# Instalação de dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc-dev g++ libffi-dev libxml2 unixodbc-dev unixodbc zlib1g curl gnupg libaio1 unzip && \
    apt-get install -y tzdata && \
    apt-get clean

# Configuração do fuso horário
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Configuração do repositório Microsoft e instalação do cliente SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 mssql-tools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Adicionando mssql-tools ao PATH
ENV PATH="${PATH}:/opt/mssql-tools/bin"

# Definição do diretório de trabalho
WORKDIR /app

# Atualização do pip
RUN pip install --upgrade pip

# Copiando o código-fonte e os requisitos do projeto
COPY src/. /app/
COPY requirements.txt /app/

# Instalação das dependências Python
RUN pip install --no-cache-dir -r /app/requirements.txt