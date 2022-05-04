import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import numpy as np

# Greenplum DataDirect Connectivity Driver downloaded from
# https://network.pivotal.io/products/vmware-tanzu-greenplum#/releases/6120/file_groups/178
connection = pyodbc.connect('DRIVER={DataDirect 7.1 Greenplum Wire Protocol};'
                            'Database=project;Server=192.168.43.159;Port=5432;',
                            user='super', password='p')
cursor = connection.cursor()
cursor.execute('SELECT fraud_dt, fraud_type FROM final_proj.report')
report_data = cursor.fetchall()
df = pd.DataFrame({'fraud_date':np.array(report_data)[:, 0], 'fraud_type':np.array(report_data)[:, 1]})
df['fraud_date'] = df['fraud_date'].dt.date
df.groupby('fraud_type')['fraud_date'].value_counts().unstack(0).plot.bar(figsize=(15, 10), rot=0)
plt.xticks(fontsize=14)
plt.yticks(range(0, 20, 5))
plt.xlabel('fraud date', fontsize=20)
plt.show()
