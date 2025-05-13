import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager

# Загрузка переменных окружения
load_dotenv()

# Конфигурация подключения к БД
DB_CONFIG = {
    'user': os.getenv('PGUSER'),
    'password': os.getenv('PGPASSWORD'),
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT'),
    'database': os.getenv('PGDATABASE')
}

# Создание пула соединений
connection_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    **DB_CONFIG
)

@contextmanager
def get_db_connection():
    """
    Контекстный менеджер для получения соединения из пула
    """
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

def get_sqlalchemy_engine():
    """
    Создание движка SQLAlchemy для работы с БД
    """
    return create_engine(
        f'postgresql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}'
    )

def test_connection():
    """
    Тестирование подключения к БД
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT version();')
                version = cur.fetchone()
                print(f"Подключение успешно! Версия PostgreSQL: {version[0]}")
        return True
    except Exception as e:
        print(f"Ошибка подключения к БД: {str(e)}")
        return False

if __name__ == "__main__":
    # Тестирование подключения при запуске файла
    test_connection() 