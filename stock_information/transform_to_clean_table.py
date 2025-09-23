import os
import json
from typing import Any, Dict, Iterable, Tuple, List

from dotenv import load_dotenv
load_dotenv()

from db.MySQL_db_connection import MySQLConn

from stock_information.industry_id import get_industry_id
from stock_information.t import (
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

def main():
    with MySQLConn(db=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT stock_id, stock_info FROM tw_stock_company_info")
            rows = cur.fetchall()

        # 準備資料
        to_insert = []
        for row in rows:
            stock_id = str(row["stock_id"])
            s = parse_stock_info(row["stock_info"])
            to_insert.append(build_clean_row(stock_id, s))

        # 批次 upsert
        with conn.cursor() as cur:
            for batch in chunked(to_insert, 1000):
                cur.executemany(UPSERT_SQL, batch)
        conn.commit()

    print(f"✅ Done. Upsert rows: {len(to_insert)}")

if __name__ == "__main__":
    main()