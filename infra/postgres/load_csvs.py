"""
Loads the 8 Home Credit Default Risk CSVs, straight out of the Kaggle
competition zip, into Postgres as tables — simulating the transactional
origination system this data would normally come from.

Usage:
    uv run infra/postgres/load_csvs.py --zip /path/to/home-credit-default-risk.zip
"""

import argparse
import io
import os
import zipfile
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

TABLES = {
    "application_train": "application_train.csv",
    "application_test": "application_test.csv",
    "bureau": "bureau.csv",
    "bureau_balance": "bureau_balance.csv",
    "previous_application": "previous_application.csv",
    "POS_CASH_balance": "POS_CASH_balance.csv",
    "installments_payments": "installments_payments.csv",
    "credit_card_balance": "credit_card_balance.csv",
}

PANDAS_TO_PG = {
    "int64": "BIGINT",
    "float64": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "object": "TEXT",
}

SCHEMA_SAMPLE_ROWS = 10_000


def infer_columns(zf: zipfile.ZipFile, member: str) -> list[tuple[str, str]]:
    with zf.open(member) as f:
        sample = pd.read_csv(f, nrows=SCHEMA_SAMPLE_ROWS)
    return [
        (col, PANDAS_TO_PG.get(str(dtype), "TEXT"))
        for col, dtype in sample.dtypes.items()
    ]


def create_table(cur, table_name: str, columns: list[tuple[str, str]]) -> None:
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    cols_sql = ", ".join(f'"{col}" {pg_type}' for col, pg_type in columns)
    cur.execute(f'CREATE TABLE "{table_name}" ({cols_sql})')


def copy_csv(conn, cur, zf: zipfile.ZipFile, member: str, table_name: str) -> int:
    with zf.open(member) as raw:
        text_stream = io.TextIOWrapper(raw, encoding="utf-8")
        cur.copy_expert(
            f'COPY "{table_name}" FROM STDIN WITH (FORMAT csv, HEADER true)',
            text_stream,
        )
    conn.commit()
    cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
    return cur.fetchone()[0]


def main() -> None:
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--zip",
        required=True,
        help="Path to the home-credit-default-risk.zip file downloaded from Kaggle",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    cur = conn.cursor()

    try:
        with zipfile.ZipFile(args.zip) as zf:
            for table_name, member in TABLES.items():
                print(f"[{table_name}] inferring schema from {member} ...")
                columns = infer_columns(zf, member)
                create_table(cur, table_name, columns)
                conn.commit()

                print(f"[{table_name}] copying rows ...")
                row_count = copy_csv(conn, cur, zf, member, table_name)
                print(f"[{table_name}] loaded {row_count:,} rows")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
