import sys
import time
import typing
import datetime
import requests
import pandas as pd

from tqdm import tqdm
from loguru import logger
from db.router import Router
from pydantic import BaseModel # for the validation of data type
from data_preparation.crawler_preparation import *


# set data types for table
class TaiwanStockPrice(BaseModel):
    StockID: str
    TradeVolume: int
    Transaction: int
    TradeValue:  int
    Open: float
    Max:  float
    Min:  float
    Close:  float
    Change: float
    Date: str


# check the data type of pd.DataFrame
def check_schema(
    df: pd.DataFrame,
) -> pd.DataFrame:

    # change data format to json
    df_dict = df.to_dict("records")

    df_schema = [
        TaiwanStockPrice(**dd).__dict__
        for dd in df_dict
    ]
    df = pd.DataFrame(df_schema)
    return df


# 建立時間列表(用於爬取所有資料)
def gen_date_list(
    start_date: str, end_date: str
) -> typing.List[str]: # 宣告日期需為文字串列

    # datetime.date(year, month, date)
    start_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d").date())
    end_date = (datetime.datetime.strptime(end_date, "%Y-%m-%d").date())
    
    days = (end_date - start_date).days + 1
    
    # 產出start_date到end_date的完整日期列表
    date_list = [
        str(start_date + datetime.timedelta(days=day)) # equals to end_date
        for day in range(days)
    ]
    return date_list


def crawler_twse(
    date: str,
) -> pd.DataFrame:

    url = (
        "https://www.twse.com.tw/exchangeReport/MI_INDEX"
        "?response=json&date={date}&type=ALL"
    )
    url = url.format(
        date=date.replace("-", "")
    )
    # 避免被證交所 ban ip, 在每次爬蟲時, 先 sleep 5 秒
    time.sleep(5)
    # request method
    res = requests.get(
        url, headers=twse_header()
    )
    if (
        res.json()["stat"]
        == "很抱歉，沒有符合條件的資料!"
    ):
        # 如果 date 是周末，會回傳很抱歉，沒有符合條件的資料!
        return pd.DataFrame()
    # 2009 年以後的資料, 股價在 response 中的 data9
    # 2009 年以後的資料, 股價在 response 中的 data8
    # 不同格式, 在證交所的資料中, 是很常見的,
    # 沒資料的情境也要考慮進去，例如現在週六沒有交易，但在 2007 年週六是有交易的
    try:
        if "data9" in res.json():
            df = pd.DataFrame(
                res.json()["data9"]
            )
            colname = res.json()[
                "fields9"
            ]
        elif "data8" in res.json():
            df = pd.DataFrame(
                res.json()["data8"]
            )
            colname = res.json()[
                "fields8"
            ]
        elif res.json()["stat"] in [
            "查詢日期小於93年2月11日，請重新查詢!",
            "很抱歉，沒有符合條件的資料!",
        ]:
            return pd.DataFrame()
    except BaseException:
        return pd.DataFrame()

    if len(df) == 0:
        return pd.DataFrame()
    # 欄位中英轉換
    df = colname_zh2en(
        df.copy(), colname
    )
    df["Date"] = date
    return df


def main(start_date: str, end_date: str):
    """
    data start from 1994-02-11
    """
    # generate the whole list
    date_list = gen_date_list(start_date, end_date)

    # connect with database
    db_router = Router()

    for date in tqdm(date_list):
        logger.info(f"crawling data of: {date}")

        # crawl by datelist
        df = crawler_twse(date=date)

        # if not empty
        if len(df) > 0:
            df = clear_data(df.copy())
            df = check_schema(df.copy())

            # upload data into database
            try:
                """
                - fail: If table exists, do nothing.
                - append: If table exists, insert data. Create if does not exist.
                - replace: If table exists, drop it, recreate it, and insert data.
                """
                df.to_sql(
                    index=False,
                    chunksize=1000,
                    if_exists="append",
                    name="TaiwanStockPrice", # table_name
                    con=db_router.mysql_financialdata_conn,
                )
            except Exception as e:
                logger.info(e)


if __name__ == "__main__":
    start_date, end_date = sys.argv[1:] # input of main(1, 2), 0=main.py
    main(start_date, end_date)
