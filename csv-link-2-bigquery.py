import os
import pandas as pd
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials/hk-re-analytics-cfdf597ce9d5.json'

csv_url = input('URL to CSV: ')
drop_option = input('Drop remarks column? (Y/N) ')
drop_first_row = input('Drop first row & remove header row? (Y/N) ')

df = pd.read_csv(csv_url)
print(df.columns)
input('Check column names list, if okay, press enter.')

if drop_first_row == 'Y':
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])

# Function to drop the remarks column.
drop_column_list = []
if drop_option == 'Y':
    column_name = df.columns
    for i in range(len(column_name)):
        if column_name[i].endswith('Remarks') == True:
            drop_column_list.append(column_name[i])
        else:
            continue

if len(drop_column_list) > 0:
    df = df.drop(columns=drop_column_list)

df = df.fillna('')
df = df.replace('-', '0')

print(df.columns)

client = bigquery.Client()

project_dataset_id = input('project_id.dataset_id: ')
table_id = input('Name the table id: ')

full_table_id = f'{project_dataset_id}.{table_id}'

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    skip_leading_rows=1,
)

job = client.load_table_from_dataframe(
    df, 
    full_table_id, 
    job_config=job_config
    )

job.result()

table = client.get_table(full_table_id)

print(
    f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {full_table_id}'
    )
