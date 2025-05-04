
import os
import psycopg2
import pandas as pd

# Danh sách các bảng cần truy vấn
table = ['statsperleagueseason', 'statsperplayerseason', 'statsplayerper90']

# Cấu hình kết nối PostgreSQL
PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "football",
    "user": "admin",
    "password": "admin123"
}

# Hàm khởi tạo kết nối
def init_connection(config):
    try:
        return psycopg2.connect(
            database=config['database'],
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port']
        )
    except psycopg2.OperationalError as e:
        print("Error connecting to PostgreSQL:", e)
        raise

# Hàm truy vấn dữ liệu
def extract_data():
    conn = init_connection(PSQL_CONFIG)
    return [pd.read_sql(f'SELECT * FROM analysis.{tab}', conn) for tab in table]