import pandas, os, json, configparser, urllib.parse, os
from sqlalchemy import create_engine
from Utils import GetColumnName, ConvertToSQLDatatype, RemoveNotExistsColumn, CheckDatatype, ConvertData, ConvertListToSQLInsert
# --------------------------------------------main--------------------------------------------
# Read data description file
path_Des = "./TEMP_DBDesc.xlsx"
fileDescription = pandas.ExcelFile(path_Des)
sheets = fileDescription.sheet_names

# Get list of excel data file
path_excelFiles = './data/'
lstFiles = os.listdir(path_excelFiles)
dctFiles = dict.fromkeys(sheets)
for key in dctFiles.keys():
    for i in range(len(lstFiles)):
        if (key + '.xlsx') in lstFiles[i]:
            dctFiles[key] = lstFiles[i]

# Load RE pattern file
with open ("./assert/ConvertToSQL.json") as file:
    dct = json.load(file)

config = configparser.ConfigParser()
config.read("./assert/SQLConnection.ini")

# Staging db engine
HOST, PORT, DB_NAME, USERNAME, PASSWORD = config['STAGING_DATABASE'].values()
params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s'%(HOST, PORT, DB_NAME, USERNAME, PASSWORD))
stg_engine = create_engine('mssql+pyodbc:///?odbc_connect=%s'%params)

# Create and insert data for staging db
for name in sheets:
    print(name, end=' ')
    # Get columns and datatype from TOPOVN_DATA_DESCRIPTION's sheet
    data = pandas.read_excel(fileDescription, sheet_name=name, header=1)
    columns = GetColumnName(list(data.columns))
    data = data.dropna(subset=[columns[0]])
    
    col_columns = [name.replace(' ', '_') for name in data[columns[0]].tolist()]
    col_datatype = [ConvertToSQLDatatype(i, dct) for i in data[columns[1]].tolist()]

    if dctFiles[name] != None:
        # Read data from excel file corresponding with excel's sheet name
        data = pandas.read_excel(path_excelFiles + dctFiles[name])
        data = data.fillna('null')

        col_columns = RemoveNotExistsColumn(data.columns, col_columns, col_datatype)

        data, col_columns, col_datatype = CheckDatatype(data, col_columns, col_datatype)

        data = data[col_columns]
        data = ConvertData(data, col_columns, col_datatype)
    
    # Create table
    sql_tablebody = ''
    for i in range(len(col_columns)):
        sql_tablebody += col_columns[i] + ' ' + col_datatype[i] + ',\n'
    command = 'create table {name} ({body})'.format(
        name = name,
        body = sql_tablebody
    )
    stg_engine.execute(command)
    if dctFiles[name] != None:
        # Insert data
        command = 'insert into {table}({columns}) values ({values})'
        for i in range(len(data)):
            row = ConvertListToSQLInsert(data.loc[i].tolist(), col_datatype)
            stg_engine.execute(command.format(
                table = name,
                columns = ','.join(col_columns),
                values = ','.join(row)
            ))
        print("DONE")
    else:
        print("created")
stg_engine.dispose()


# DWH engine
HOST, PORT, DB_NAME, USERNAME, PASSWORD = config['DWH_DATAMART'].values()
params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s'%(HOST, PORT, DB_NAME, USERNAME, PASSWORD))
dwh_engine = create_engine('mssql+pyodbc:///?odbc_connect=%s'%params)

# Create table for DWH
path_Des = "./DWH_DBDesc.xlsx"
fileDescription = pandas.ExcelFile(path_Des)
sheets = fileDescription.sheet_names
print('DWH')
for name in sheets:
    data = pandas.read_excel(fileDescription, sheet_name=name, header=1)
    columns = GetColumnName(list(data.columns))
    data = data.dropna(subset=[columns[0]])
    
    col_columns = [name.replace(' ', '_') for name in data[columns[0]].tolist()]
    col_datatype = [ConvertToSQLDatatype(i, dct) for i in data[columns[1]].tolist()]
    # Create table
    print(name, end=' ')
    sql_tablebody = ''
    for i in range(len(col_columns)):
        sql_tablebody += col_columns[i] + ' ' + col_datatype[i] + ',\n'
    command = 'create table {name} ({body})'.format(
        name = name,
        body = sql_tablebody
    )
    dwh_engine.execute(command)
    print('created')

dwh_engine.dispose()