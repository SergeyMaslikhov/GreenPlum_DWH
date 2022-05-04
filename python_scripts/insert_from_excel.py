import pyodbc
import argparse
import pandas as pd
import datetime


def transform_data(cell):
    if type(cell) == float:
        return str(cell)
    elif type(cell) in (str, int):
        return f"\'{cell}\'"
    elif type(cell) == datetime.date:
        return f"to_date(\'{cell}\', \'YYYY-MM-DD\')"
    return f"to_timestamp(\'{cell}\', \'YYYY-MM-DD HH24:MI:SS\')"


# parsing file path
parser = argparse.ArgumentParser()
parser.add_argument("-f")
args = parser.parse_args()

df = pd.read_excel(args.f)
# transform from timestamp to date
df['date_of_birth'] = df['date_of_birth'].dt.date
df['account_valid_to'] = df['account_valid_to'].dt.date
# filter data for the last day
df = df[df['date'].dt.date == df['date'].dt.date.max()]

# generate insert query
insert = '''INSERT INTO final_proj.denormalized 
            (trans_id, trans_date, card_num, account, account_valid_to, client, 
            last_name, first_name, patrinymic, date_of_birth, passport, passport_valid_to, 
            phone, oper_type, amount, oper_result, terminal, terminal_type, city, address) VALUES '''
insert += ',\n'.join(
                    list(
                        map(lambda x: "(" + ", ".join(map(transform_data, x)) + ")", df.values)
                    )
) + ';'

# Greenplum DataDirect Connectivity Driver downloaded from
# https://network.pivotal.io/products/vmware-tanzu-greenplum#/releases/6120/file_groups/178
connection = pyodbc.connect('DRIVER={DataDirect 7.1 Greenplum Wire Protocol};'
                            'Database=project;Server=192.168.43.159;Port=5432;',
                            user='super', password='p')
cursor = connection.cursor()
cursor.execute(insert)
connection.commit()
connection.close()
