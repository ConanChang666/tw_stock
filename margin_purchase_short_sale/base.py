# margin_purchase_short_sale/base.py
import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_API = os.getenv("FINMIND_API")
HEADERS = {"Authorization": f"Bearer {FINMIND_API}"} if FINMIND_API else {}

# FinMind -> 你的資料表（snake_case）欄位映射
FINMIND_TO_SNAKE = {
    "MarginPurchaseBuy": "margin_purchase_buy",
    "MarginPurchaseCashRepayment": "margin_purchase_cash_repayment",
    "MarginPurchaseLimit": "margin_purchase_limit",
    "MarginPurchaseSell": "margin_purchase_sell",
    "MarginPurchaseTodayBalance": "margin_purchase_today_balance",
    "MarginPurchaseYesterdayBalance": "margin_purchase_yesterday_balance",
    "Note": "note",
    "OffsetLoanAndShort": "offset_loan_and_short",
    "ShortSaleBuy": "short_sale_buy",
    "ShortSaleCashRepayment": "short_sale_cash_repayment",
    "ShortSaleLimit": "short_sale_limit",
    "ShortSaleSell": "short_sale_sell",
    "ShortSaleTodayBalance": "short_sale_today_balance",
    "ShortSaleYesterdayBalance": "short_sale_yesterday_balance",
}

SNAKE_EXPECTED = [
    "date",
    "stock_id",
    "margin_purchase_buy",
    "margin_purchase_cash_repayment",
    "margin_purchase_limit",
    "margin_purchase_sell",
    "margin_purchase_today_balance",
    "margin_purchase_yesterday_balance",
    "note",
    "offset_loan_and_short",
    "short_sale_buy",
    "short_sale_cash_repayment",
    "short_sale_limit",
    "short_sale_sell",
    "short_sale_today_balance",
    "short_sale_yesterday_balance",
]

def fetch_margin_short(stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    params = {
        "dataset": "TaiwanStockMarginPurchaseShortSale",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    resp = requests.get(URL, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    res = resp.json()
    if "data" not in res or not res["data"]:
        return pd.DataFrame()
    return pd.DataFrame(res["data"])

def finmind_to_snake(df: pd.DataFrame) -> pd.DataFrame:
    """將 FinMind 欄位轉成你 DB 使用的 snake_case，補齊缺欄、格式化日期。"""
    if df.empty:
        return df.copy()
    df = df.copy()
    df.rename(columns=FINMIND_TO_SNAKE, inplace=True)
    for col in SNAKE_EXPECTED:
        if col not in df.columns:
            df[col] = pd.NA
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")
    remain = [c for c in df.columns if c not in SNAKE_EXPECTED]
    out = df[SNAKE_EXPECTED + remain].sort_values("date", kind="stable")
    # 輕量稽核欄位（入庫前可丟掉）
    out["_fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["_source"] = "FinMind"
    return out

def df_nulls_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """把 NaN/pd.NA 轉 None，方便 PyMySQL 寫入。"""
    if df.empty:
        return df
    return df.where(pd.notnull(df), None)