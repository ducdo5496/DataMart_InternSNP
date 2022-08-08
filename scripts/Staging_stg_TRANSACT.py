import urllib.parse, sys
from configparser import ConfigParser
from sqlalchemy import create_engine

config = ConfigParser()
try:
    config.read("./assert/SQLConnection.ini")
    # Create engine for staging db
    HOST, PORT, DB_NAME, USERNAME, PASSWORD = config['STAGING_DATABASE'].values()
    params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s'%(HOST, PORT, DB_NAME, USERNAME, PASSWORD))
    stg_engine = create_engine('mssql+pyodbc:///?odbc_connect=%s'%params)

    # Create engine for dwh
    HOST, PORT, DB_NAME, USERNAME, PASSWORD = config['DWH_DATAMART'].values()
    params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s'%(HOST, PORT, DB_NAME, USERNAME, PASSWORD))
    dwh_engine = create_engine('mssql+pyodbc:///?odbc_connect=%s'%params)
except Exception as error:
    print('Could not load SQL config')
    sys.exit()

import pandas
item = pandas.read_sql('select * from ITEM', stg_engine)
item_reefer = pandas.read_sql('select * from ITEM_REEFER', stg_engine)
item_dangerous = pandas.read_sql('select * from ITEM_DANGEROUS', stg_engine)
item_oog = pandas.read_sql('select * from ITEM_OOG', stg_engine)
item_location = pandas.read_sql('select * from ITEM_LOCATION', stg_engine)
yard_area = pandas.read_sql('select * from YARD_AREA', stg_engine)
pregate_transact = pandas.read_sql('select * from PREGATE_TRANSACT', stg_engine)
print('fetch done')

item = item.loc[
    (item['SITE_ID'].str.strip()=='CTL') &
    (item['ARR_BY'].str.strip()=='T') &
    (item['HIST_FLG'].str.strip()=='Y') &
    (item['FEL'].str.strip()=='F')
]
item = item.drop(['ITEM_TYPE'], axis=1)
item = item[['ITEM_KEY', 'ITEM_NO', 'LENGTH', 'ARR_TS', 'DEP_TS', 'PLACE_OF_DELIVERY', 'PLACE_OF_RECEIPT']]
item = item.reset_index(drop=True)

item_location = item_location[['ITEM_KEY', 'STACK', 'EXEC_TS']]


item_reefer['ITEM_TYPE'] = 'R'
item_reefer = pandas.merge(item_reefer, item, how='left', on='ITEM_KEY')
item_reefer = item_reefer[['ITEM_KEY', 'ITEM_TYPE']]

item_dangerous['ITEM_TYPE'] = 'D'
item_dangerous = pandas.merge(item_dangerous, item, how='left', on='ITEM_KEY')
item_dangerous = item_dangerous[['ITEM_KEY', 'ITEM_TYPE']]

item_oog['ITEM_TYPE'] = 'O'
item_oog = pandas.merge(item_oog, item, how='left', on='ITEM_KEY')
item_oog = item_oog[['ITEM_KEY', 'ITEM_TYPE']]

print('ITEM_SPECIAL created')
item_special = pandas.concat([item_reefer, item_dangerous, item_oog], axis=0)
item = pandas.merge(item, item_special, how='left', on='ITEM_KEY').fillna({'ITEM_TYPE' : 'G'})

item['DURATION'] = item['DEP_TS'] - item['ARR_TS']
item['DURATION'] = item['DURATION'].apply(pandas.Timedelta.total_seconds)

pregate_RECEIPT = pregate_transact.loc[pregate_transact['R_D'].str.strip()=='R']
item_RECEIPT = pandas.merge(item, pregate_RECEIPT, how='inner', on='ITEM_KEY', suffixes=('_item', '_pregate'))
item_RECEIPT = pandas.merge(item_RECEIPT, item_location, how='inner', on='ITEM_KEY')
# for i in range(len(item_RECEIPT)):
#     row = item_RECEIPT.loc[i]
#     sample = item_location.loc[
#         (item_location['ITEM_KEY'] == row['ITEM_KEY']) &
#         (item_location['EXEC_TS'] > row['ARR_TS'])
#     ].sort_values(['EXEC_TS'], ascending=[True])
#     lstSTACK = sample['STACK'].tolist()
#     while len(lstSTACK)>0:
#         if lstSTACK[0] == None:
#             lstSTACK.pop(0)
#         else:
#             item_RECEIPT.loc[i, 'STACK'] = lstSTACK[0]
#             break
#     else:
#         pass
item_RECEIPT = item_RECEIPT[['ITEM_KEY', 'ITEM_NO_item', 'LENGTH', 'AREA', 'STACK', 'R_D', 'DURATION', 'ARR_TS', 'ITEM_TYPE']]
item_RECEIPT = item_RECEIPT.rename(columns={
    'ARR_TS' : 'EXEC_TS'
})
print('ITEM_RECEPIT created')

pregate_DEPORT = pregate_transact.loc[pregate_transact['R_D'].str.strip()=='D']
item_DEPORT = pandas.merge(item, pregate_DEPORT, how='inner', on='ITEM_KEY', suffixes=('_item', '_pregate'))
item_DEPORT = pandas.merge(item_DEPORT, item_location, how='inner', on='ITEM_KEY')
# for i in range(len(item_DEPORT)):
#     row = item_DEPORT.loc[i]
#     sample = item_location.loc[
#         (item_location['ITEM_KEY'] == row['ITEM_KEY']) &
#         (item_location['EXEC_TS'] > row['DEP_TS'])
#     ].sort_values(['EXEC_TS'], ascending=[True])
#     lstSTACK = sample['STACK'].tolist()
#     while len(lstSTACK)>0:
#         if lstSTACK[0] == None:
#             lstSTACK.pop(0)
#         else:
#             item_DEPORT.loc[i, 'STACK'] = lstSTACK[0]
#             break
#     else:
#         pass
item_DEPORT = item_DEPORT[['ITEM_KEY', 'ITEM_NO_item', 'LENGTH', 'AREA', 'STACK', 'R_D', 'DURATION', 'DEP_TS', 'ITEM_TYPE']]
item_RECEIPT = item_RECEIPT.rename(columns={
    'DEP_TS' : 'EXEC_TS'
})
print('ITEM_DEPORT created')

stg_transact = pandas.concat([item_RECEIPT, item_DEPORT], axis=0)
stg_transact = stg_transact.rename(columns={'ITEM_NO_item' : 'ITEM_NO'})
stg_transact = stg_transact.astype({
    'EXEC_TS' : 'datetime64',
    'DURATION' : 'int'
})

import pandas.io.sql as psql
print('stg_TRANSACT inserting', end=' ')
psql.to_sql(frame=stg_transact, name='stg_TRANSACT', con=stg_engine, if_exists='append', index=False)
print('DONE')
stg_engine.dispose()

unique_location = set([tuple(row) for i, row in yard_area[['STACK', 'AREA', 'REGION']].iterrows()])
while len (unique_location) > 0:
    dwh_engine.execute('insert into d_LOCATION values {}'.format(unique_location.pop()))
print('d_LOCATION inserted')

unique_year = set([row['ARR_TS'].to_pydatetime().year for i, row in stg_transact.iterrows()])
sample = []
for year in unique_year:
    for month in range(1, 13):
        row = []
        if month < 10:
            row.append(str(year)+'0'+str(month))
        else:
            row.append(str(year)+str(month))
        row.append(year)
        row.append(month)
        sample.append(row)
d_date = pandas.DataFrame(sample, columns=['DATE_ID', 'YEAR', 'MONTH'])
psql.to_sql(d_date, name='d_DATE', con=dwh_engine, if_exists='append', index=False)

from Utils import StripText
d_location = pandas.read_sql('select * from d_LOCATION', dwh_engine)
d_location['AREA'] = d_location['AREA'].apply(StripText)
d_location['STACK'] = d_location['STACK'].apply(StripText)

stg_transact['AREA'] = stg_transact['AREA'].apply(StripText)
stg_transact['STACK'] = stg_transact['STACK'].apply(StripText)

stg_transact = pandas.merge(stg_transact, d_location, how='inner', on=['AREA', 'STACK'])
stg_transact = stg_transact.drop(['REGION'], axis=1)

lstLOCATION = set([tuple(row) for i, row in stg_transact[['EXEC_TS', 'LOC_ID', 'R_D', 'LENGTH', 'ITEM_TYPE']].iterrows()])

print('Create fact table')
f_transact = pandas.DataFrame([list(i) for i in lstLOCATION], columns = ['EXEC_TS', 'LOC_ID', 'R_D', 'LENGTH', 'ITEM_TYPE'])
f_transact['QUANTITY'] = None
f_transact['SUM_DURATION'] = None

for i in range(len(f_transact)):
    row = f_transact.loc[i]
    sample = stg_transact.loc[
        (stg_transact['EXEC_TS'] == row['EXEC_TS']) &
        (stg_transact['LOC_ID'] == row['LOC_ID']) &
        (stg_transact['R_D'] == row['R_D']) &
        (stg_transact['LENGTH'] == row['LENGTH']) &
        (stg_transact['ITEM_TYPE'] == row['ITEM_TYPE'])
    ]
    quantity = len(sample)
    sum_duration = sum(sample['DURATION'].tolist())
    f_transact.loc[i, 'QUANTITY'] = quantity
    f_transact.loc[i, 'SUM_DURATION'] = sum_duration

    year, month = row['EXEC_TS'].to_pydatetime().year, row['EXEC_TS'].to_pydatetime().month
    date_id = d_date.loc[
        d_date['YEAR'] == year &
        d_date['MONTH'] == month
        ]['DATE_ID'][0]
    f_transact.loc[i, 'EXEC_TS'] = date_id


psql.to_sql(frame=f_transact, name='f_TRANSACT', con=dwh_engine, if_exists='append', index=False)
print('Inserted f_TRANSACT')
dwh_engine.execute('alter table d_LOCATION add constraint PK_LOC_ID primary key (LOC_ID)')
dwh_engine.execute('alter table d_DATE add constraint PK_DATE_ID primary key (DATE_ID)')
dwh_engine.execute('alter table f_TRANSACT add constraint FK_LOCID_d_LOCATION foreign key (LOC_ID) references d_LOCATION(LOC_ID)')
dwh_engine.execute('alter table f_TRANSACT add constraint FK_DATEID_D_DATE foreign key (DATE_ID) references d_DATE(DATE_ID)')

dwh_engine.dispose()