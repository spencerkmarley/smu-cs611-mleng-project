import functions_framework
import google.cloud.storage as storage
import json
import pandas as pd
import requests

from datetime import datetime, timezone, timedelta
from google.cloud.storage import Blob

# requirements.txt
# functions-framework==3.*
# google.cloud.storage
# pandas
# requests

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
  '''Returns the coordinates of all taxis via the LTA API endpoint for a given datetime string
  '''

  now = datetime.now(timezone(timedelta(hours=8)))
  date_time = now.strftime("%Y-%m-%dT%H:%M:%S")
  file_name = now.strftime("%Y-%m-%dT%H-%M-%S")+".json"

  client = storage.Client(project="ml-eng-cs611-group-project")
  bucket = client.get_bucket("ml-eng-cs611-group-project-taxis")

  url = "https://api.data.gov.sg/v1/transport/taxi-availability"
  params={'date_time':date_time}
  response=requests.get(url, params=params)
  json_data = response.json()
  string_data = json.dumps(json_data)
  bucket.blob("taxis/"+file_name).upload_from_string(string_data)

  return None