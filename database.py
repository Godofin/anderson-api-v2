"""
Configuração do banco de dados Neon Postgres
"""
import os
import pg8000.native
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

def get_connection():
    """Cria uma nova conexão com o Neon Postgres usando pg8000"""
    database_url = os.getenv("POSTGRES_URL")
    
    if not database_url:
        raise ValueError("POSTGRES_URL não encontrada nas variáveis de ambiente")
    
    try:
        parsed = urlparse(database_url)
        
        conn = pg8000.native.Connection(
            user=parsed.username,
            password=parsed.password,
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:],
            ssl_context=True
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao Neon Postgres: {e}")
        raise


def execute_query(query: str, params: tuple = None, fetch: str = "all"):
    """Executa uma query no banco de dados"""
    conn = None
    try:
        conn = get_connection()
        
        if params:
            result = conn.run(query, params)
        else:
            result = conn.run(query)
        
        if fetch == "all":
            columns = [desc[0] for desc in conn.columns]
            return [dict(zip(columns, row)) for row in result]
        elif fetch == "one":
            if result:
                columns = [desc[0] for desc in conn.columns]
                return dict(zip(columns, result[0]))
            return None
        else:
            return None
            
    except Exception as e:
        print(f"❌ Erro na query: {e}")
        raise e
    finally:
        if conn:
            conn.close()