"""
Configuração do banco de dados Neon Postgres
"""
import os
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

load_dotenv()

class Database:
    _pool: Optional[SimpleConnectionPool] = None
    
    @classmethod
    def initialize(cls):
        """Inicializa o pool de conexões com o Neon Postgres"""
        if cls._pool is None:
            database_url = os.getenv("POSTGRES_URL")
            
            if not database_url:
                raise ValueError("POSTGRES_URL não encontrada nas variáveis de ambiente")
            
            try:
                cls._pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    dsn=database_url,
                    cursor_factory=RealDictCursor
                )
                print("✅ Pool de conexões Neon Postgres inicializado")
            except Exception as e:
                print(f"❌ Erro ao conectar ao Neon Postgres: {e}")
                raise
    
    @classmethod
    def get_connection(cls):
        """Obtém uma conexão do pool"""
        if cls._pool is None:
            cls.initialize()
        return cls._pool.getconn()
    
    @classmethod
    def return_connection(cls, conn):
        """Devolve a conexão ao pool"""
        if cls._pool:
            cls._pool.putconn(conn)
    
    @classmethod
    def close_all(cls):
        """Fecha todas as conexões do pool"""
        if cls._pool:
            cls._pool.closeall()
            print("🔒 Pool de conexões fechado")


class DatabaseConnection:
    """Context manager para gerenciar conexões automaticamente"""
    def __enter__(self):
        self.conn = Database.get_connection()
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        Database.return_connection(self.conn)


def execute_query(query: str, params: tuple = None, fetch: str = "all"):
    """
    Executa uma query no banco de dados
    
    Args:
        query: SQL query
        params: Parâmetros da query
        fetch: 'all', 'one', ou 'none'
    """
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            
            if fetch == "all":
                return cursor.fetchall()
            elif fetch == "one":
                return cursor.fetchone()
            elif fetch == "none":
                return None