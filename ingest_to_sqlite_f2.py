# pandas-based ingestion for ecommerce dataset
import sqlite3
import sys
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data_f2"
DB_PATH = BASE_DIR / "ecommerce_f2.db"

TABLE_ORDER = [
    "segment_taxonomy_f2",
    "product_master_f2",
    "client_directory_f2",
    "transaction_log_f2",
    "transaction_details_f2",
]

CREATE_STATEMENTS = {
    "segment_taxonomy_f2": """
        CREATE TABLE segment_taxonomy_f2 (
            segment_id TEXT PRIMARY KEY,
            segment_name TEXT NOT NULL,
            parent_segment_id TEXT,
            FOREIGN KEY (parent_segment_id) REFERENCES segment_taxonomy_f2(segment_id)
        )
    """,
    "product_master_f2": """
        CREATE TABLE product_master_f2 (
            product_id TEXT PRIMARY KEY,
            sku TEXT NOT NULL,
            name TEXT NOT NULL,
            segment_id TEXT NOT NULL,
            price REAL NOT NULL,
            cost REAL NOT NULL,
            weight_kg REAL NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (segment_id) REFERENCES segment_taxonomy_f2(segment_id)
        )
    """,
    "client_directory_f2": """
        CREATE TABLE client_directory_f2 (
            client_id TEXT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT NOT NULL,
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            country TEXT NOT NULL,
            signup_date TEXT NOT NULL
        )
    """,
    "transaction_log_f2": """
        CREATE TABLE transaction_log_f2 (
            transaction_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            transaction_date TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            status TEXT NOT NULL,
            transaction_total REAL NOT NULL,
            FOREIGN KEY (client_id) REFERENCES client_directory_f2(client_id)
        )
    """,
    "transaction_details_f2": """
        CREATE TABLE transaction_details_f2 (
            detail_id TEXT PRIMARY KEY,
            transaction_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY (transaction_id) REFERENCES transaction_log_f2(transaction_id),
            FOREIGN KEY (product_id) REFERENCES product_master_f2(product_id)
        )
    """,
}

INDEX_STATEMENTS = [
    "CREATE INDEX idx_product_segment_f2 ON product_master_f2(segment_id)",
    "CREATE INDEX idx_transaction_log_client_f2 ON transaction_log_f2(client_id)",
    "CREATE INDEX idx_transaction_details_transaction_f2 ON transaction_details_f2(transaction_id)",
    "CREATE INDEX idx_transaction_details_product_f2 ON transaction_details_f2(product_id)",
]

CSV_MAPPING = {
    "segment_taxonomy_f2": "segment_taxonomy_f2.csv",
    "product_master_f2": "product_master_f2.csv",
    "client_directory_f2": "client_directory_f2.csv",
    "transaction_log_f2": "transaction_log_f2.csv",
    "transaction_details_f2": "transaction_details_f2.csv",
}

def read_csv(table_name: str) -> pd.DataFrame:
    csv_path = DATA_DIR / CSV_MAPPING[table_name]
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV file: {csv_path}")
    return pd.read_csv(csv_path)

def main() -> None:
    try:
        if DB_PATH.exists():
            DB_PATH.unlink()

        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON;")

        with conn:
            for table in reversed(TABLE_ORDER):
                conn.execute(f"DROP TABLE IF EXISTS {table}")

            for table in TABLE_ORDER:
                conn.execute(CREATE_STATEMENTS[table])

            for table in TABLE_ORDER:
                df = read_csv(table)
                df.to_sql(table, conn, if_exists="append", index=False)

            for stmt in INDEX_STATEMENTS:
                conn.execute(stmt)

        conn.close()
        print("Ingest complete")
    except Exception as exc:
        print(f"Ingest error: {exc}")
        sys.exit(1)

if __name__ == "__main__":
    main()
