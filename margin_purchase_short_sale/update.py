# margin_purchase_short_sale/update.py
from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Iterable, Optional, Tuple
import time
import pandas as pd

from margin_purchase_short_sale.base import fetch_margin_short, finmind_to_snake, df_nulls_to_none
from margin_purchase_short_sale.ticker import tickers
from db.MySQL_db_connection import MySQLConn  # 你的連線池模組

# ===== 設定 =====
DB_NAME = "stock_market_data_lake"
TABLE = "taiwan_stock_margin_purchase_short_sale"

START_FALLBACK = "2001-01-01"   # DB 無資料時的起點
CHUNK_DAYS = 120                # 分段抓的天數
SLEEP_BETWEEN_CALLS = 0.3       # 每段間隔，避免打太兇

# ===== 建表（保險）=====
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    date DATE NOT NULL,
    stock_id VARCHAR(10) NOT NULL,

    margin_purchase_buy BIGINT,
    margin_purchase_cash_repayment BIGINT,
    margin_purchase_limit BIGINT,
    margin_purchase_sell BIGINT,
    margin_purchase_today_balance BIGINT,
    margin_purchase_yesterday_balance BIGINT,
    note VARCHAR(255),
    offset_loan_and_short BIGINT,
    short_sale_buy BIGINT,
    short_sale_cash_repayment BIGINT,
    short_sale_limit BIGINT,
    short_sale_sell BIGINT,
    short_sale_today_balance BIGINT,
    short_sale_yesterday_balance BIGINT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (stock_id, date),
    KEY idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

def ensure_table_exists():
    with MySQLConn(DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

def _daterange_chunks(start: str, end: str, step_days: int):
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    cur = s
    delta = timedelta(days=step_days)
    while cur <= e:
        chunk_end = min(cur + delta - timedelta(days=1), e)
        yield cur.isoformat(), chunk_end.isoformat()
        cur = chunk_end + timedelta(days=1)

def _get_db_max_date(stock_id: str) -> Optional[str]:
    sql = f"SELECT MAX(date) AS max_date FROM {TABLE} WHERE stock_id=%s;"
    with MySQLConn(DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (stock_id,))
            row = cur.fetchone()
            if row and row.get("max_date"):
                md = row["max_date"]
                if isinstance(md, (datetime, date)):
                    return md.strftime("%Y-%m-%d")
                return str(md)
    return None

def _ensure_missing_columns(df: pd.DataFrame):
    """若 DF 有表內缺的欄位，自動 ALTER TABLE 補上（以資料型態猜 BIGINT/DOUBLE/VARCHAR）。"""
    if df.empty:
        return
    with MySQLConn(DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SHOW COLUMNS FROM {TABLE};")
            existing = {r["Field"] for r in cur.fetchall()}
            alter_parts = []
            for col, dtype in df.dtypes.items():
                if col in ("stock_id", "date"):
                    continue
                if col not in existing:
                    if pd.api.types.is_integer_dtype(dtype):
                        coltype = "BIGINT"
                    elif pd.api.types.is_float_dtype(dtype):
                        coltype = "DOUBLE"
                    else:
                        coltype = "VARCHAR(255)"
                    alter_parts.append(f"ADD COLUMN `{col}` {coltype} NULL")
            if alter_parts:
                cur.execute(f"ALTER TABLE {TABLE} " + ", ".join(alter_parts) + ";")
        conn.commit()

def _upsert_df_to_mysql(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    # 轉 snake_case、丟掉暫存欄位、處理 Null
    df = finmind_to_snake(df)
    cols = [
        "date", "stock_id",
        "margin_purchase_buy", "margin_purchase_cash_repayment", "margin_purchase_limit",
        "margin_purchase_sell", "margin_purchase_today_balance", "margin_purchase_yesterday_balance",
        "note", "offset_loan_and_short",
        "short_sale_buy", "short_sale_cash_repayment", "short_sale_limit",
        "short_sale_sell", "short_sale_today_balance", "short_sale_yesterday_balance",
    ]
    df = df[cols + [c for c in df.columns if c not in cols]]  # 保留未知欄位讓 _ensure_missing_columns 看得到
    _ensure_missing_columns(df)  # 先補表欄位
    data = df[cols].copy()
    data = df_nulls_to_none(data)

    sql = f"""
    INSERT INTO {TABLE} (
        date, stock_id,
        margin_purchase_buy, margin_purchase_cash_repayment, margin_purchase_limit,
        margin_purchase_sell, margin_purchase_today_balance, margin_purchase_yesterday_balance,
        note, offset_loan_and_short,
        short_sale_buy, short_sale_cash_repayment, short_sale_limit,
        short_sale_sell, short_sale_today_balance, short_sale_yesterday_balance
    ) VALUES (
        %(date)s, %(stock_id)s,
        %(margin_purchase_buy)s, %(margin_purchase_cash_repayment)s, %(margin_purchase_limit)s,
        %(margin_purchase_sell)s, %(margin_purchase_today_balance)s, %(margin_purchase_yesterday_balance)s,
        %(note)s, %(offset_loan_and_short)s,
        %(short_sale_buy)s, %(short_sale_cash_repayment)s, %(short_sale_limit)s,
        %(short_sale_sell)s, %(short_sale_today_balance)s, %(short_sale_yesterday_balance)s
    )
    ON DUPLICATE KEY UPDATE
        margin_purchase_buy=VALUES(margin_purchase_buy),
        margin_purchase_cash_repayment=VALUES(margin_purchase_cash_repayment),
        margin_purchase_limit=VALUES(margin_purchase_limit),
        margin_purchase_sell=VALUES(margin_purchase_sell),
        margin_purchase_today_balance=VALUES(margin_purchase_today_balance),
        margin_purchase_yesterday_balance=VALUES(margin_purchase_yesterday_balance),
        note=VALUES(note),
        offset_loan_and_short=VALUES(offset_loan_and_short),
        short_sale_buy=VALUES(short_sale_buy),
        short_sale_cash_repayment=VALUES(short_sale_cash_repayment),
        short_sale_limit=VALUES(short_sale_limit),
        short_sale_sell=VALUES(short_sale_sell),
        short_sale_today_balance=VALUES(short_sale_today_balance),
        short_sale_yesterday_balance=VALUES(short_sale_yesterday_balance),
        updated_at=CURRENT_TIMESTAMP;
    """
    with MySQLConn(DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, data.to_dict(orient="records"))
        conn.commit()
    return len(data)

def run_full_history_for_symbol(stock_id: str, end_date: Optional[str] = None) -> int:
    """單檔股票：從 DB 最新日+1 續抓，或從 2001-01-01 到今天。"""
    end = end_date or date.today().isoformat()
    max_in_db = _get_db_max_date(stock_id)
    start = (datetime.strptime(max_in_db, "%Y-%m-%d").date() + timedelta(days=1)).isoformat() if max_in_db else START_FALLBACK
    if start > end:
        print(f"  - {stock_id} 已最新（DB 最新到 {max_in_db}）")
        return 0

    print(f"  ↳ 抓取區間：{start} ~ {end}（分段 {CHUNK_DAYS} 天）")
    total_written = 0

    for s, e in _daterange_chunks(start, end, CHUNK_DAYS):
        try:
            df = fetch_margin_short(stock_id, s, e)
        except Exception as ex:
            print(f"    ! API 失敗 {stock_id} {s}~{e}: {ex}")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        if df.empty:
            print(f"    - 無資料  {s} ~ {e}")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        written = _upsert_df_to_mysql(df)
        total_written += written
        print(f"    ✓ 寫入 {written} 筆  {s} ~ {e}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    return total_written

def run_full_history(symbols: Optional[Iterable[str]] = None) -> None:
    """多檔股票：全歷史（或自動續抓）"""
    ensure_table_exists()
    syms = list(tickers.keys()) if symbols is None else list(symbols)
    end = date.today().isoformat()

    print(f"== TaiwanStockMarginPurchaseShortSale 歷史資料抓取（直到 {end}） ==")
    for i, sid in enumerate(syms, 1):
        print(f"[{i}/{len(syms)}] {sid}")
        total = run_full_history_for_symbol(sid, end_date=end)
        print(f"  ◎ {sid} 完成：本次寫入 {total} 筆\n")

if __name__ == "__main__":
    # 不帶參數就全跑：從 2001-01-01（或 DB 最新日+1）抓到今天，直接插庫
    run_full_history()