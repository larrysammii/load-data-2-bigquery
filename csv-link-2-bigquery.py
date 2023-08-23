import os
import pandas as pd
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials/hk-re-analytics-cfdf597ce9d5.json'

csv_url = input('URL to CSV: ')
df = pd.read_csv(csv_url)
df = df.fillna('')
df = df.replace('-', '0')
print(df)

client = bigquery.Client()

project_dataset_id = input('project_id.dataset_id: ')
table_id = input('Name the table id: ') # TODO: function to name it automatically

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
