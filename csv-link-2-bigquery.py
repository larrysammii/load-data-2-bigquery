import os
import time
import io
import requests
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials/hk-re-analytics-cfdf597ce9d5.json'

csv_url = input('URL to CSV: ')

client = bigquery.Client()

project_dataset_id = input('project_id.dataset_id: ')
table_id = 'Name the table id: ' # TODO: function to name it automatically

full_table_id = "{}.{}".format(project_dataset_id, table_id)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    skip_leading_rows=1,
)

response = requests.get(csv_url)
if response.status_code != 200:
    print(response.reason)
else:
    content_byte = response.content
    bytes_io = io.BytesIO(content_byte)
    job = client.load_table_from_file(bytes_io, full_table_id, job_config=job_config)

    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
        print(job.state)
    
    print(job.result())

    table = client.get_table(full_table_id)
    print(
        'Loaded {} rows and {} columns to {}'.format(
            table.num_rows, len(table.schema), full_table_id
    ))