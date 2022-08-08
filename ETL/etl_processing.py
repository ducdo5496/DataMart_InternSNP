import pyodbc as odbc
import pandas as pd

# Importing dataset from excel 
dir_path = 'E:/Intern/baitoan/pack/Data_SNP_Intern/CSV/'
item_data = pd.read_csv(dir_path+'2_ITEM.csv')
location_data = pd.read_csv(dir_path+'3_ITEM_LOCATION.csv')
reefer_data = pd.read_csv(dir_path+'4_ITEM_REEFER.csv')
oog_data = pd.read_csv(dir_path+'5_ITEM_OOG.csv')
dangerous_data = pd.read_csv(dir_path+'6_ITEM_DANGEROUS.csv')
pregate_transact_data = pd.read_csv(dir_path+'7_PREGATE_TRANSACT.csv')
yard_area_data = pd.read_csv(dir_path+'11_YARD_AREA.csv')


# Convert data to DataFrame
df_item = pd.DataFrame(item_data) 
df_location = pd.DataFrame(location_data) 
df_reefer = pd.DataFrame(reefer_data) 
df_oog = pd.DataFrame(oog_data) 
df_dangerous = pd.DataFrame(dangerous_data) 
df_pregate_transact = pd.DataFrame(pregate_transact_data)
df_yard_area = pd.DataFrame(yard_area_data) 


# Data Clean up
df_item['ARR_TS'] = pd.to_datetime(df_item['ARR_TS']).dt.strftime('%Y-%m-%d %H:%M:%S')
df_item['DEP_TS'] = pd.to_datetime(df_item['DEP_TS']).dt.strftime('%Y-%m-%d %H:%M:%S')
df_item.drop(df_item.query('PLACE_OF_DELIVERY.isnull()').index, inplace=True)
df_item.drop(df_item.query('PLACE_OF_RECEIPT.isnull()').index, inplace=True)
df_item.drop(df_item.query('ITEM_KEY < 10000000'), inplace= True, errors= 'ignore')
df_item.drop_duplicates(inplace=True)

df_location.drop(df_location.query('ITEM_KEY < 10000000'), inplace= True, errors= 'ignore')
df_location.drop(df_location.query('STACK.isnull()').index, inplace=True)
df_location.drop_duplicates(inplace=True)

df_reefer.drop(df_reefer.query('ITEM_KEY < 10000000'), inplace= True, errors= 'ignore')
df_reefer.drop_duplicates(inplace=True)

df_oog.drop(df_oog.query('ITEM_KEY < 10000000'), inplace= True, errors= 'ignore')
df_oog.drop_duplicates(inplace=True)

df_dangerous.drop(df_dangerous.query('ITEM_KEY < 10000000'), inplace= True, errors= 'ignore')
df_dangerous.drop_duplicates(inplace=True)

df_pregate_transact.drop(df_pregate_transact.query('ITEM_KEY < 10000000'), inplace= True, errors= 'ignore')
df_pregate_transact.drop(df_pregate_transact.query('RECEIVAL_PLACE.isnull()').index, inplace=True)
df_pregate_transact.drop(df_pregate_transact.query('PLACE_OF_DELIVERY.isnull()').index, inplace=True)
df_pregate_transact.drop(df_pregate_transact.query('R_D.isnull()').index, inplace=True)
df_pregate_transact.drop(df_pregate_transact.query('AREA.isnull()').index, inplace=True)
df_pregate_transact.drop_duplicates(inplace=True)

df_yard_area.drop_duplicates(inplace=True)


# Select the desired data column
columns_item = ['ITEM_KEY','ITEM_NO','SITE_ID', 'ARR_BY', 'ARR_TS', 'FEL','DEP_TS',
   'DEP_BY','LENGTH','PLACE_OF_DELIVERY','PLACE_OF_RECEIPT','ITEM_TYPE','HIST_FLG']
df_item_data = df_item[columns_item]
records_item =df_item_data.values.tolist()

columns_location = ['ITEM_KEY', 'STACK']
df_location_data = df_location[columns_location]
records_location =df_location_data.values.tolist()

columns_reefer = ['ITEM_KEY']
df_reefer_data = df_reefer[columns_reefer]
records_reefer =df_reefer_data.values.tolist()

columns_oog = ['ITEM_KEY']
df_oog_data = df_oog[columns_oog]
records_oog =df_oog_data.values.tolist()

columns_dangerous = ['ITEM_KEY']
df_dangerous_data = df_dangerous[columns_dangerous]
records_dangerous =df_dangerous_data.values.tolist()

columns_pregate_transact = ['ITEM_KEY', 'ITEM_NO', 'R_D', 'AREA', 
        'RECEIVAL_PLACE', 'PLACE_OF_DELIVERY', 'FEL', 'HIST_FLG']
df_pregate_transact_data = df_pregate_transact[columns_pregate_transact]
records_pregate_transact =df_pregate_transact_data.values.tolist()

columns_yard_area = ['AREA', 'STACK', 'REGION']
df_yard_area_data = df_yard_area[columns_yard_area]
records_yard_area =df_yard_area_data.values.tolist()

# Write script insert statement
sql_insert_item = """
    INSERT INTO stg_ITEM 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

sql_insert_location = """
    INSERT INTO stg_ITEM_LOCATION
    VALUES (?, ?)
"""

sql_insert_reefer =  """
    INSERT INTO stg_ITEM_REEFER
    VALUES (?)
"""

sql_insert_oog =  """
    INSERT INTO stg_ITEM_OOG
    VALUES (?)
"""

sql_insert_dangerous =  """
    INSERT INTO stg_ITEM_DANGEROUS
    VALUES (?)
"""

sql_insert_pregate_transact =  """
    INSERT INTO stg_ITEM_PREGATE_TRANSACT
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

sql_insert_yard_area =  """
    INSERT INTO stg_ITEM_YARD_AREA
    VALUES (?, ?, ?)
"""


# Insert Data to SQL
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
except odbc.DatabaseError as e:
    print('Database Error: ')
    print(str(e[1]))
except odbc.Error as e:
    print('Connection Error: ')
    print(str(e[1]))
else: 
    cursor = conn.cursor()

sql_insert = [sql_insert_item, sql_insert_location, sql_insert_reefer, sql_insert_oog, sql_insert_dangerous, sql_insert_pregate_transact, sql_insert_yard_area]
records = [records_item, records_location, records_reefer, records_oog, records_dangerous, records_pregate_transact, records_yard_area]

for i in range(len(records)):
    try:
        cursor.executemany(sql_insert[i], records[i])
        cursor.commit()
    except Exception as e:
        cursor.rollback()
        print(str(e[1]))

cursor.close()
conn.close()

print("Done !!!")


