@echo off
setlocal enabledelayedexpansion

REM 
if exist .venv (
  rmdir /s /q .venv
)

python -m venv .venv
call .venv\Scripts\activate.bat

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

python src\main.py