"""Download Banorte XB reconciliation data from Snowflake to Parquet.

Usage:
    uv run scripts/download_banorte_xb.py
    uv run scripts/download_banorte_xb.py --start-date 2025-01-01 --end-date 2025-06-30
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from churn_research.extraction.downloader import SnowflakeDownloader
from churn_research.extraction.query_builder import QuerySpec

TABLE = "REC_2024_03_14_15_54_45_9E849DE6832248BBAE31133205A4B895"
DATABASE = "DB_EBANX_MEXICO_522FB63E48224E93BB1EEB9C51FDAAB5"
SCHEMA = "PUBLIC"

COLUMNS = [
    "a_a_column_38",   # card_type
    "a_a_left94",      # fee_date
    "a_a_column_39",   # card_issuer
    "a_a_column_11",   # operation_type
    "a_a_ifelse86",    # amount_gross
    "a_math116",       # amount_installment
    "a_a_column_9",    # provider
    "a_a_column_15",   # installments
    "a_a_column_13",   # payment_amount
    "a_a_column_19",   # merchant_name
    "a_a_column_37",   # card_scheme
    "a_a_left74",      # confirm_date
    "a_a_ifelse80",    # camara_compensacion
    "a_b_i",           # custo_transacao
    "a_math114",       # fee_ebanx
    "a_ifelse117",     # amount_iva_installment
    "a_ifelse118",     # net_amount
    "a_math115",       # iva_ebanx
    "b_column_11",     # afiliacion (side B)
]

ALIASES = {
    "a_a_column_38": "card_type",
    "a_a_left94": "fee_date",
    "a_a_column_39": "card_issuer",
    "a_a_column_11": "operation_type",
    "a_a_ifelse86": "amount_gross",
    "a_math116": "amount_installment",
    "a_a_column_9": "provider",
    "a_a_column_15": "installments",
    "a_a_column_13": "payment_amount",
    "a_a_column_19": "merchant_name",
    "a_a_column_37": "card_scheme",
    "a_a_left74": "confirm_date",
    "a_a_ifelse80": "camara_compensacion",
    "a_b_i": "custo_transacao",
    "a_math114": "fee_ebanx",
    "a_ifelse117": "amount_iva_installment",
    "a_ifelse118": "net_amount",
    "a_math115": "iva_ebanx",
    "b_column_11": "afiliacion",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Banorte XB data to Parquet")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2024, 1, 1))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2025, 12, 31))
    parser.add_argument("--output-dir", default="data/raw/banorte_xb")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    args = parse_args()

    spec = QuerySpec(
        database=DATABASE,
        schema=SCHEMA,
        table=TABLE,
        columns=COLUMNS,
        column_aliases=ALIASES,
        filters={"status": "RECONCILED"},
        date_column="a_a_left74",
        start_date=args.start_date,
        end_date=args.end_date,
    )

    logging.info("Downloading %s → %s", spec.full_table_name, args.output_dir)
    logging.info("Date range: %s to %s", args.start_date, args.end_date)

    downloader = SnowflakeDownloader()
    files = downloader.download(spec, output_dir=args.output_dir)

    logging.info("Done! %d file(s) created", len(files))


if __name__ == "__main__":
    main()
