"""Postgres connection for ingestion"""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

def connect():
    return psycopg2.connect(
        host=os.environ["INGESTION_DB_HOST"],
        port=os.environ["INGESTION_DB_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"]
    )