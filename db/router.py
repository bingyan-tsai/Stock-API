

import time
import typing

from loguru import logger
from sqlalchemy import engine
from db.clients import get_mysql_financialdata_conn


# check the connection before usage
def check_alive(
    connect: engine.base.Connection,
):
    connect.execute("SELECT 1+1")


# re-connect if lost connection with MySQL
def reconnect(
    connect_func: typing.Callable,
) -> engine.base.Connection:

    try:
        connect = connect_func()

    except Exception as e:
        logger.info(
            f"{connect_func.__name__} reconnect error {e}"
        )
    return connect


def check_connect_alive(
    connect: engine.base.Connection,
    connect_func: typing.Callable,
):
    if connect:
        try:
            check_alive(connect)
            return connect
            
        except Exception as e:
            logger.info(
                f"{connect_func.__name__} connect, error: {e}"
            )
            time.sleep(1)

            # try again if lost connection 
            connect = reconnect(
                connect_func
            )
            return check_connect_alive(
                connect, connect_func
            )
    else:
        connect = reconnect(
            connect_func
        )
        return check_connect_alive(
            connect, connect_func
        )


class Router:
    def __init__(self):
        self._mysql_financialdata_conn = (
            get_mysql_financialdata_conn()
        )

    def check_mysql_financialdata_conn_alive(self):
        self._mysql_financialdata_conn = check_connect_alive(
            self._mysql_financialdata_conn,
            get_mysql_financialdata_conn,
        )
        return self._mysql_financialdata_conn


    @property
    def mysql_financialdata_conn(self):
        """
        使用 property，在每次拿取 connect 時，
        都先經過 check alive 檢查 connect 是否活著
        """
        return self.check_mysql_financialdata_conn_alive()
