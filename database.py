"""
Configuração do banco de dados Neon Postgres
"""
import os
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
    """Executa uma query no banco de dados"""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(query, params)
        
        if fetch == "all":
            results = cursor.fetchall()
            return results if results else []
        elif fetch == "one":
            result = cursor.fetchone()
            return result
        else:
            conn.commit()
            return None
            
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Erro na query: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()