import time
import requests
import pandas as pd

from loguru import logger
from data_preparation.crawler_preparation import *


def crawler_twse(
    date: str,
) -> pd.DataFrame:

    logger.info("crawler_twse")
    
    date2 = date.replace("-", "")
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date2}&type=ALL"
    
    # to avoid anti-crawler
    time.sleep(3)
    
    # request method
    res = requests.get(url, headers=twse_header())
    
    # 2009 年以後的資料, 股價在 response 中的 data9
    # 2009 年以後的資料, 股價在 response 中的 data8
    # 不同格式, 在證交所的資料中, 是很常見的,
    # 沒資料的情境也要考慮進去，例如現在週六沒有交易，但在 2007 年週六是有交易的

    df = pd.DataFrame()
    
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
            pass
        
    except Exception as e:
        logger.error(e)
        return pd.DataFrame()

    if len(df) == 0:
        return pd.DataFrame()
    
    # 欄位中英轉換
    df = colname_zh2en(
        df.copy(), colname
    )
    
    df["Date"] = date
    df = convert_change(df.copy())
    df = clear_data(df.copy())

    print(df)

    return df


if __name__ == "__main__":
    crawler_twse("2023-01-17")
