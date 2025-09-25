import os
import json
from typing import Any, Dict, Iterable, Tuple, List

from dotenv import load_dotenv
load_dotenv()

from db.MySQL_db_connection import MySQLConn
from stock_information.translate_to_en import (
    build_multilang_name,
    build_multilang_address,
    build_multilang_description,
)

DB_NAME = "stock_market_data_lake"

def resolve_market(s: Dict[str, Any]) -> str:
    m = (s.get("marketName") or "").strip()
    if m:
        if "上市" in m:
            return "上市"
        if "上櫃" in m:
            return "上櫃"
        return m
    if s.get("listingDate"):
        return "上市"
    if s.get("OTCDate"):
        return "上櫃"
    return "未上市櫃"

def parse_stock_info(val: Any) -> Dict[str, Any]:
    """stock_info 可能是 str(JSON) 或 dict，統一成 dict。"""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except Exception:
        return {}

def build_clean_row(stock_id: str, s: Dict[str, Any]) -> Tuple:
    # 多語系欄位
    stock_name = build_multilang_name(
        zh_tw=s.get("companyName"),
        en_from_field=s.get("companyEnglishName"),
    )

    address = build_multilang_address(
        zh_tw=s.get("address"),
        en_street=s.get("englishAddress_Street"),
        en_county=s.get("englishAddress_County"),
    )

    description = build_multilang_description(
        zh_tw=s.get("mainBusiness"),
        en_prefill=None,
    )

    # industry_id 先設 None，交給後面的 cross-DB SQL 自動補
    industry_id = None

    market = resolve_market(s)
    office_site = s.get("internetAddress") or None

    return (
        stock_id,
        json.dumps(stock_name, ensure_ascii=False),
        industry_id,
        market,
        "TW",
        "TWD",
        office_site,
        json.dumps(address, ensure_ascii=False),
        json.dumps(description, ensure_ascii=False),
    )

# 用 COALESCE 避免 None 覆蓋現有值
UPSERT_SQL = """
INSERT INTO tw_stock_company_info_clean
(stock_id, stock_name, industry_id, market, country, currency, office_website, address, description)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  stock_name = VALUES(stock_name),
  industry_id = COALESCE(VALUES(industry_id), industry_id),
  market = VALUES(market),
  country = VALUES(country),
  currency = VALUES(currency),
  office_website = VALUES(office_website),
  address = VALUES(address),
  description = VALUES(description),
  updated_at = CURRENT_TIMESTAMP
"""

# 每次啟動最後都要跑的 cross-DB 更新
UPDATE_FROM_STOCK_DEMO_SQL = """
UPDATE stock_market_data_lake.tw_stock_company_info_clean AS c
JOIN financial_statement.stock_demo AS d
  ON JSON_VALID(d.value) = 1
 AND JSON_UNQUOTE(JSON_EXTRACT(d.value, '$.symbol')) = c.stock_id
JOIN stock_market_data_lake.tw_industry_mapping AS m
  ON m.industry_name = JSON_UNQUOTE(JSON_EXTRACT(d.value, '$.industry'))
SET c.industry_id = m.industry_id,
    c.updated_at  = CURRENT_TIMESTAMP
WHERE c.industry_id IS NULL
   OR c.industry_id <> m.industry_id;
"""

def chunked(iterable: Iterable, size: int) -> Iterable[List]:
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

import pymysql

READ_BATCH_LIMIT = None
UPSERT_BATCH_SIZE = 100

def main():
    # 1) 讀取來源資料
    print("[DB] fetch raw rows...")
    with MySQLConn(db=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT stock_id, stock_info FROM tw_stock_company_info")
            rows = cur.fetchall()
    if READ_BATCH_LIMIT:
        rows = rows[:READ_BATCH_LIMIT]
    print(f"[DB] fetched rows: {len(rows)}")

    # 2) 組裝要 upsert 的資料
    print("[BUILD] building rows ...")
    to_insert = []
    for idx, row in enumerate(rows, 1):
        stock_id = str(row["stock_id"])
        s = parse_stock_info(row["stock_info"])
        to_insert.append(build_clean_row(stock_id, s))
        if idx % 100 == 0:
            print(f"[BUILD] {idx}/{len(rows)}")

    # 3) upsert
    print("[DB] upserting...")
    inserted = 0
    with MySQLConn(db=DB_NAME) as conn:
        with conn.cursor() as cur:
            for bi, batch in enumerate(chunked(to_insert, UPSERT_BATCH_SIZE), 1):
                try:
                    conn.ping(reconnect=True)
                except Exception:
                    pass
                try:
                    cur.executemany(UPSERT_SQL, batch)
                except pymysql.err.OperationalError as e:
                    if e.args and e.args[0] in (2006, 2013):
                        print(f"[WARN] connection lost on batch {bi}, retrying once...")
                        conn.ping(reconnect=True)
                        cur.executemany(UPSERT_SQL, batch)
                    else:
                        raise
                except pymysql.err.DataError as e:
                    print(f"[WARN] DataError on batch {bi}, fallback to single insert: {e}")
                    for rec in batch:
                        cur.execute(UPSERT_SQL, rec)
                conn.commit()
                inserted += len(batch)
                if bi % 10 == 0:
                    print(f"[UPSERT] batch {bi} ({inserted}/{len(to_insert)}) committed")

    print(f"[DB] upsert done. rows: {inserted}")

    # 4) 每次啟動後立即進行 cross-DB 更新
    print("[DB] syncing industry_id from financial_statement.stock_demo ...")
    with MySQLConn(db=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute(UPDATE_FROM_STOCK_DEMO_SQL)
            conn.commit()
    print("[DB] industry_id synced.")

    print("Done.")

if __name__ == "__main__":
    main()