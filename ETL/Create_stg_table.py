import pyodbc as odbc
import pandas as pd 

DRIVER = 'SQL Server'
SERVER_NAME = 'DESKTOP-VDV5RCH\SQLEXPRESS'
DATABASE_NAME = 'ETL_PROCESSING'

conn_string = f"""
    Driver={{{DRIVER}}};
    Server={SERVER_NAME};
    Database={DATABASE_NAME};
    Trusted_Connection=no;
    Uid=sa;
    Pwd=12345678;
"""

try:
    conn = odbc.connect(conn_string)
except Exception as e:
    print(e)
    print('Its Over!!!')
else:
    cursor = conn.cursor()

    create_stg_ITEM = """
        CREATE TABLE stg_ITEM (
            ITEM_KEY NUMERIC(9),
            ITEM_NO CHAR(12),
            SITE_ID CHAR(6), 
            ARR_BY CHAR(1),
            ARR_TS DATETIME,
            FEL CHAR(1), 
            DEP_TS DATETIME,
            DEP_BY CHAR(1),
            LENGTH NUMERIC(9),
            PLACE_OF_DELIVERY CHAR(8),
            PLACE_OF_RECEIPT CHAR(8),
            ITEM_TYPE CHAR(2),
            HIST_FLG CHAR(1),
        )
    """

    create_stg_ITEM_LOCATION = """
        CREATE TABLE stg_ITEM_LOCATION (
            ITEM_KEY NUMERIC(9),
            STACK CHAR(3),
        )
    """

    create_stg_ITEM_REEFER = """
        CREATE TABLE stg_ITEM_REEFER (
            ITEM_KEY NUMERIC(9),
        )
    """

    create_stg_ITEM_OOG = """
        CREATE TABLE stg_ITEM_OOG (
            ITEM_KEY NUMERIC(9),
        )
    """

    create_stg_ITEM_DANGEROUS = """
        CREATE TABLE stg_ITEM_DANGEROUS (
            ITEM_KEY NUMERIC(9),
        )
    """

    create_stg_ITEM_PREGATE_TRANSACT = """
        CREATE TABLE stg_ITEM_PREGATE_TRANSACT (
            ITEM_KEY NUMERIC(9),
            ITEM_NO CHAR(12),
            R_D CHAR(1),
            AREA CHAR(5),   
            RECEIVAL_PLACE CHAR(5),
            PLACE_OF_DELIVERY CHAR(8),   
            FEL CHAR(1),
            HIST_FLG CHAR(1),
        )
    """

    create_stg_ITEM_YARD_AREA = """
        CREATE TABLE stg_ITEM_YARD_AREA (
            AREA CHAR(5),
            STACK CHAR(3),
            REGION CHAR(3),

        )
    """

    create_list = (
        create_stg_ITEM,
        create_stg_ITEM_DANGEROUS,
        create_stg_ITEM_LOCATION,
        create_stg_ITEM_OOG,
        create_stg_ITEM_PREGATE_TRANSACT,
        create_stg_ITEM_REEFER,
        create_stg_ITEM_YARD_AREA,
    )

    for i in create_list:
        try:   
            cursor.execute(i)
            conn.commit()
        except Exception as e:
            cursor.rollback()
            print(e)
            print('Transaction rolled back')
        else:
            conn.commit()
    
    conn.close()
