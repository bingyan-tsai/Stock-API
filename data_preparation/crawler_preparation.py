import typing
import pandas as pd

from loguru import logger


# headers for twse_crawler
def twse_header():
    return {
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.twse.com.tw",
        "Origin": "https://www.twse.com.tw",
        "Referer": "https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


# 將 DataFrame 的欄位名稱轉換為英文
def colname_zh2en(
    df: pd.DataFrame, # 宣告應投入的資料型態
    colname: typing.List[str], # 欄位名稱為文字列表
) -> pd.DataFrame: # 投入應為(->)pd.DataFrame的結構
    
    taiwan_stock_price = {
        "證券代號": "StockID",
        "證券名稱": "",
        "成交股數": "TradeVolume",
        "成交筆數": "Transaction",
        "成交金額": "TradeValue",
        "開盤價": "Open",
        "最高價": "Max",
        "最低價": "Min",
        "收盤價": "Close",
        "漲跌(+/-)": "Dir",
        "漲跌價差": "Change",
        "最後揭示買價": "",
        "最後揭示買量": "",
        "最後揭示賣價": "",
        "最後揭示賣量": "",
        "本益比": "",
    }
    
    df.columns = [
        taiwan_stock_price[col]
        for col in colname
    ]
    # 移除不要的欄位(用空名稱)
    df = df.drop([""], axis=1)
    return df


# 轉換漲跌幅號
def convert_change(
    df: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("convert_change")
    
    # 只留漲跌幅號(+ or -)
    df["Dir"] = (
        df["Dir"]
        .str.split(">")
        .str[1]
        .str.split("<")
        .str[0]
    )
    df["Change"] = (
        df["Dir"] + df["Change"]
    )
    
    df["Change"] = (
        df["Change"]
        .str.replace(" ", "")
        .str.replace("X", "")
        .astype(float)
    )

    df = df.fillna("")
    df = df.drop(["Dir"], axis=1)
    return df


# 資料清理
def clear_data(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """資料清理, 將文字轉成數字"""
    for col in [
        "TradeVolume",
        "Transaction",
        "TradeValue",
        "Open",
        "Max",
        "Min",
        "Close",
        "Change",
    ]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "")
            .str.replace("X", "")
            .str.replace("+", "")
            .str.replace("----", "0")
            .str.replace("---", "0")
            .str.replace("--", "0")
            .str.replace(" ", "")
            .str.replace("除權息", "0")
            .str.replace("除息", "0")
            .str.replace("除權", "0")
        )
    return df
