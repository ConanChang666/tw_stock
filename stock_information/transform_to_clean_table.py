import os
import json
from typing import Any, Dict, Iterable, Tuple, List

from dotenv import load_dotenv
load_dotenv()

from db.MySQL_db_connection import MySQLConn

from stock_information.industry_id import get_industry_id
from stock_information.translate_to_en import (
    build_multilang_name,
    build_multilang_address,
    build_multilang_description,
)

DB_NAME = "stock_market_data_lake" 

# --- 市場別判斷：優先使用 stock_info.marketName，否則用日期推斷 ---
def resolve_market(s: Dict[str, Any]) -> str:
    m = (s.get("marketName") or "").strip()
    if m:
        # 常見格式: "上市公司" / "上櫃公司"
        if "上市" in m:
            return "上市"
        if "上櫃" in m:
            return "上櫃"
        # 其他字樣（如 興櫃公司），直接回傳原始/簡化
        return m

    # 後備規則
    if s.get("listingDate"):
        return "上市"
    if s.get("OTCDate"):
        return "上櫃"
    return "未上市櫃"

def parse_stock_info(val: Any) -> Dict[str, Any]:
    """stock_info 可能是 str(JSON) 或 dict，這裡統一成 dict。"""
    if val is None:
        return {}
    if isinstance(val, (dict,)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return {}

def build_clean_row(stock_id: str, s: Dict[str, Any]) -> Tuple:
    # 多語系欄位
    stock_name = build_multilang_name(
        zh_tw=s.get("companyName"),             # 公司中文全名
        en_from_field=s.get("companyEnglishName")  # 公司英文全名
    )

    address = build_multilang_address(
        zh_tw=s.get("address"),
        en_street=s.get("englishAddress_Street"),
        en_county=s.get("englishAddress_County"),
    )

    description = build_multilang_description(
        zh_tw=s.get("mainBusiness"),
        en_prefill=None,  # 若你另有英文描述欄位，可填入這裡
    )

    # 產業 id
    industry_id = get_industry_id(s.get("industryCategory"))

    # 市場別
    market = resolve_market(s)

    # 其它欄位
    office_site = s.get("internetAddress") or None

    # 回傳為 executemany 用的 tuple，順序要對齊 SQL
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

UPSERT_SQL = """
INSERT INTO tw_stock_company_info_clean
(stock_id, stock_name, industry_id, market, country, currency, office_website, address, description)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  stock_name = VALUES(stock_name),
  industry_id = VALUES(industry_id),
  market = VALUES(market),
  country = VALUES(country),
  currency = VALUES(currency),
  office_website = VALUES(office_website),
  address = VALUES(address),
  description = VALUES(description),
  updated_at = CURRENT_TIMESTAMP
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

READ_BATCH_LIMIT = None   # 例如先測試 500；None = 全部
UPSERT_BATCH_SIZE = 100   # 小批量，避免 max_allowed_packet

def main():
    # ---- (1) 先讀，立刻關閉連線，避免閒置超時 ----
    print("[DB] fetch raw rows...")
    with MySQLConn(db=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT stock_id, stock_info FROM tw_stock_company_info")
            rows = cur.fetchall()
    if READ_BATCH_LIMIT:
        rows = rows[:READ_BATCH_LIMIT]
    print(f"[DB] fetched rows: {len(rows)}")

    # ---- (2) 本地處理（含翻譯）----
    print("[BUILD] building rows (may take time due to translation)...")
    to_insert = []
    for idx, row in enumerate(rows, 1):
        stock_id = str(row["stock_id"])
        s = parse_stock_info(row["stock_info"])
        to_insert.append(build_clean_row(stock_id, s))
        if idx % 100 == 0:
            print(f"[BUILD] {idx}/{len(rows)}")

    # ---- (3) 寫入前再開新連線，小批量 upsert，必要時自動重連 ----
    print("[DB] upserting...")
    inserted = 0
    with MySQLConn(db=DB_NAME) as conn:
        with conn.cursor() as cur:
            for bi, batch in enumerate(chunked(to_insert, UPSERT_BATCH_SIZE), 1):
                # 寫入前 ping：若已被關掉會自動重連
                try:
                    conn.ping(reconnect=True)
                except Exception:
                    pass

                try:
                    cur.executemany(UPSERT_SQL, batch)
                except pymysql.err.OperationalError as e:
                    # 2006/2013: server gone away / lost connection → 重連後重試一次
                    if e.args and e.args[0] in (2006, 2013):
                        print(f"[WARN] connection lost on batch {bi}, retrying once...")
                        conn.ping(reconnect=True)
                        cur.executemany(UPSERT_SQL, batch)
                    else:
                        raise
                except pymysql.err.DataError as e:
                    # 可能是單筆過大（max_allowed_packet），改逐筆寫入
                    print(f"[WARN] DataError on batch {bi}, fallback to single insert: {e}")
                    for rec in batch:
                        cur.execute(UPSERT_SQL, rec)

                conn.commit()
                inserted += len(batch)
                if bi % 10 == 0:
                    print(f"[UPSERT] batch {bi} (total {inserted}/{len(to_insert)}) committed")

    print(f"✅ Done. Upsert rows: {inserted}")

if __name__ == "__main__":
    main()