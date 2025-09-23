import os
import sys
import json
import re
import time
from pathlib import Path
from typing import Optional, Dict, Any
import random
import requests

# 讓我們能 import 上層的模組
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from db.MySQL_db_connection import MySQLConn
from .ticker import tickers  # 你的 tickers 字典

MOPS_URL = "https://mops.twse.com.tw/mops/api/t05st03"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://mops.twse.com.tw",
    "Referer": "https://mops.twse.com.tw/mops/web/t05st03",
}

def remove_number_commas(text: str) -> str:
    """去掉數字中的逗號，但保留其餘內容"""
    return re.sub(r'(?<=\d),(?=\d)', '', text)

def flatten_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """把 MOPS 回傳的 {key: {value, isHidden}} 轉成 {key: value}"""
    out = {}
    for k, v in result.items():
        if isinstance(v, dict) and "value" in v:
            val = v["value"]
            if isinstance(val, str):
                val = remove_number_commas(val)
            out[k] = val
        else:
            out[k] = v
    return out

def fetch_company(company_id: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """抓一檔公司資料；若為 ETF 或無公司名則回傳 None"""
    payload = {"companyId": company_id}
    r = requests.post(MOPS_URL, headers=HEADERS, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    if data.get("code") != 200:
        print(f"{company_id} 查詢失敗：{data.get('message')}")
        return None

    result = data.get("result") or {}
    company_name = (result.get("companyName") or {}).get("value") if isinstance(result.get("companyName"), dict) else None
    if not company_name:
        print(f"{company_id} 無公司名稱（可能是 ETF），自動略過")
        return None

    return flatten_result(result)

def upsert_one(db_name: str, stock_id: str, stock_info: Dict[str, Any]) -> None:
    """將資料以 JSON 寫入 MySQL；若已存在就更新"""
    sql = """
    INSERT INTO tw_stock_company_info (stock_id, stock_info)
    VALUES (%s, CAST(%s AS JSON))
    ON DUPLICATE KEY UPDATE
      stock_info = VALUES(stock_info),
      updated_at = CURRENT_TIMESTAMP;
    """
    payload_json = json.dumps(stock_info, ensure_ascii=False)
    with MySQLConn(db=db_name) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (stock_id, payload_json))
        conn.commit()

def main():
    db_name = "stock_market_data_lake" # 帶入實際資料庫名稱
    codes = list(tickers.keys())  # 取出所有代號

    for code in codes:
        try:
            print(f"抓取 {code} ...")
            flat = fetch_company(code)
            if flat is None:  # ETF 或無資料
                continue
            upsert_one(db_name, code, flat)
            print(f"已寫入 DB：{code}")
        except Exception as e:
            print(f"失敗 {code}: {e}")
        time.sleep(random.uniform(1.0, 2.0))  

    print("全部完成")

if __name__ == "__main__":
    main()