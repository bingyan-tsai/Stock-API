

from config import *
from sqlalchemy import create_engine, engine


# create a connection with MySQL
def get_mysql_financialdata_conn() -> engine.base.Connection:
    address = (
        # mysql+pymysql://root:test@127.0.0.1:3309/financialdata
        f"mysql+pymysql://{MYSQL_DATA_USER}:{MYSQL_DATA_PASSWORD}"
        f"@{MYSQL_DATA_HOST}:{MYSQL_DATA_PORT}/{MYSQL_DATA_DATABASE}"
    )
    
    engine = create_engine(address)
    connect = engine.connect()
    return connect
