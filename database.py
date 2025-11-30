"""
Configuração do banco de dados Neon Postgres via HTTP
"""
import os
import json
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

def execute_query(query: str, params: tuple = None, fetch: str = "all"):
    """
    Executa query usando Neon's HTTP API
    """
    import urllib.request
    import urllib.parse
    
    # Extrai informações da connection string
    database_url = os.getenv("POSTGRES_URL")
    
    if not database_url:
        raise ValueError("POSTGRES_URL não encontrada")
    
    # Parse da URL
    from urllib.parse import urlparse
    parsed = urlparse(database_url)
    
    # Monta a query com parâmetros
    if params:
        # Substitui %s por valores escapados
        query_with_params = query
        for param in params:
            if isinstance(param, str):
                escaped = param.replace("'", "''")
                query_with_params = query_with_params.replace('%s', f"'{escaped}'", 1)
            elif isinstance(param, bool):
                query_with_params = query_with_params.replace('%s', str(param), 1)
            elif param is None:
                query_with_params = query_with_params.replace('%s', 'NULL', 1)
            else:
                query_with_params = query_with_params.replace('%s', str(param), 1)
    else:
        query_with_params = query
    
    # Executa query usando psycopg2 puro (sem binary)
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:],
            user=parsed.username,
            password=parsed.password,
            sslmode='require'
        )
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, params)
        
        if fetch == "all":
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return results if results else []
        elif fetch == "one":
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result
        else:
            conn.commit()
            cursor.close()
            conn.close()
            return None
            
    except ImportError:
        raise Exception("psycopg2 não instalado")
    except Exception as e:
        print(f"Erro: {e}")
        print(f"Query: {query_with_params}")
        raise


def get_connection():
    """Retorna connection string"""
    return os.getenv("POSTGRES_URL")