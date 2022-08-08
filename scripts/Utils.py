import re, pandas
from datetime import datetime

def CleanText(text:str):
    text = text.replace('\n', '\r\n')
    text = text.replace('_x000D_', '')
    text = text.replace('"', '')
    return text

def GetColumnName(lst:list):
    patt_col = 'column(.)*'
    patt_data = 'data type'
    col_columns = list(filter(lambda x: re.match(patt_col, x.lower()), lst))
    col_datatype = list(filter(lambda x: re.match(patt_data, x.lower()), lst))
    return [col_columns.pop(0), col_datatype.pop(0)]

def ConvertToSQLDatatype(text:str, dct:dict):
    text = text.lower().replace(' (', '(').replace(', ', ',')
    for key in dct.keys():
        matched = re.findall(key, text)
        if len(matched) > 0:
            replaced, replace_with = dct[key]
            if replaced == '':
                text = replace_with
            else:
                text = text.replace(replaced, replace_with)
    return text

def ConvertData(data:pandas.DataFrame, col_columns:list, col_datatype:list):
    for i in range(len(col_datatype)):
        if col_datatype[i] == 'datetime':
            row = 0
            while row < len(data):
                try:
                    date = data.loc[row, col_columns[i]]
                    date = pandas.to_datetime(date)
                    data.loc[row, col_columns[i]] = date
                except pandas.errors.OutOfBoundsDatetime as error_outofDatetime:
                    data.loc[row, col_columns[i]] = datetime(1900, 12, 31, 23)
                except:
                    data = data.drop(row)
                    data = data.reset_index()
                row += 1
            data = data.astype({col_columns[i] : 'datetime64'})
        elif 'char' in col_datatype[i]:
            data[col_columns[i]] = data[col_columns[i]].apply(CleanText)
        elif 'numeric' in col_datatype[i]:
            data[col_columns[i]] = data[col_columns[i]].apply(float)
    try:
        data = data.drop(['index'], axis=1)
    except KeyError:
        pass
    return data

def ConvertListToSQLInsert(row:list, col_datatype:list):
    result = []
    for i in range(len(col_datatype)):
        if row[i] == 'null':
            result.append(row[i])
        elif 'varchar' in col_datatype[i]:
            result.append("N'%s'"%(row[i]))
        elif 'char' in col_datatype[i]:
            result.append("'%s'"%(row[i]))
        elif col_datatype[i] == 'datetime':
            result.append("'%s'"%(str(row[i])))
        else:
            result.append(str(row[i]))
    return result

def CheckDatatype(data:pandas.DataFrame, col_columns:list, col_datatype:list, change=True):
    for i in range(len(col_datatype)):
        if 'char' in col_datatype[i]:
            try:
                data[col_columns[i]] = data[col_columns[i]].apply(str)
                str_type = col_datatype[i]
                numOfChar = str_type[str_type.index('(')+1 : str_type.index(')')]
                lstLen = [len(text) for text in data[col_columns[i]].unique().tolist()]
                if max(lstLen) > int(numOfChar):
                    col_datatype[i] = col_datatype[i].replace(numOfChar, str(max(lstLen)))
            except:
                raise TypeError('Cant convert {0} to {1}'.format(col_columns[i], col_datatype[i]))
    return data, col_columns, col_datatype

def RemoveNotExistsColumn(columns:list, col_columns:list, col_datatype:list):
    i = 0
    while i < len(col_columns):
        if col_columns[i] not in list(columns):
            col_columns.remove(col_columns[i])
            col_datatype.remove(col_datatype[i])
        else:
             i += 1
    return col_columns

def StripText(text):
    try:
        return text.strip()
    except:
        return text