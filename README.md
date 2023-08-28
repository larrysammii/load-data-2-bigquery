# BigQuery Productivity Scripts

### A collection of scripts written in Python for BigQuery to boost productivity.

> Before executing the script, please following instructions from BigQuery and establish the connection to the BigQuery API:

```python
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "<your.json file path>"
```

#### Upload CSV from link to BigQuery (csv-link-2-bigquery.py)

As the name implies, with the download link to the CSV file, this script will upload to your dataset automatically. Could've overcomplicated the script with all the clean up scripts but I figured it would probably be better to it on BigQuery with SQL.

Inputs:

1. Link to CSV
2. Project ID and Dataset ID in project_id.dataset_id format
3. Name the Table ID
4. Done!

#### API json to BigQuery (json-link-2-bigquery.ipynb)

Upload directly from APIs to BigQuery, modify the .ipynb to you liking.

#### To-dos / Ideas:

- [ ] Excel convestion to CSV then upload
- [x] JSON Upload
- [ ] Check folder for new CSV/JSON file, then upload.
- [ ] After all the above, modularise(idk if that's a word)
