"""
Configuração do banco de dados Neon Postgres
"""
import os
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Cria uma nova conexão com o Neon Postgres"""
    database_url = os.getenv("POSTGRES_URL")
    
    if not database_url:
        raise ValueError("POSTGRES_URL não encontrada nas variáveis de ambiente")
    
    try:
        conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao Neon Postgres: {e}")
        raise


def execute_query(query: str, params: tuple = None, fetch: str = "all"):
    """
    Executa uma query no banco de dados
    
    Args:
        query: SQL query
        params: Parâmetros da query
        fetch: 'all', 'one', ou 'none'
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            
            if fetch == "all":
                result = cursor.fetchall()
            elif fetch == "one":
                result = cursor.fetchone()
            else:
                result = None
            
            conn.commit()
            return result
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()