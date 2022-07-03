import functions_framework
import google.cloud.storage as storage
import json
import pandas as pd
import pandas_gbq
import requests
import re

from datetime import datetime, timezone, timedelta
from google.cloud.storage import Blob

# requirements.txt
# functions-framework==3.*
# google.cloud.storage
# pandas
# pandas-gbq
# requests

def jsonParser(event, context):
    '''Parses NEA JSON file that hits the Google Cloud Storage buckets and uploads the metadata and readings to their respective BigQuery tables
    '''
    client = storage.Client(project="ml-eng-cs611-group-project")
    bucket = client.get_bucket("ml-eng-cs611-group-project-nea")
    
    file = event
    path = file['name']
    blob = bucket.blob(path)
    data = json.loads(blob.download_as_string(client=None))
    
    filename_pattern = r"\d\d\d\d-\d\d-\d\dT\d\d-\d\d-\d\d"
    re_timestamp = re.compile(filename_pattern)

    measure_pattern = r"(.*)\/"
    re_measure=re.compile(measure_pattern)
    
    items = data['items'][0]
    timestamp = re_timestamp.findall(path)[0]
    measure = re_measure.findall(path)[0]
    timestamp = datetime.strptime(timestamp[:16], '%Y-%m-%dT%H-%M')

    df_items = pd.DataFrame({
        'timestamp':[timestamp for i in range(len(items['readings']))],
        'station_id':[[(k,v) for k,v in obj.items()][0][1] for obj in items['readings']],
        'value':[[(k,v) for k,v in obj.items()][1][1] for obj in items['readings']],
        'Description':[measure for i in range(len(items['readings']))]})
    
    df_items['value']=df_items['value'].astype('float')
    df_items['timestamp']=pd.to_datetime(df_items['timestamp'])    

    metadata = data['metadata']
    pattern = r"\d\d\d\d-\d\d-\d\dT\d\d-\d\d-\d\d"
    re_timestamp = re.compile(pattern)
    timestamp = re_timestamp.findall(path)[0]
    timestamp = datetime.strptime(timestamp[:16], '%Y-%m-%dT%H-%M')
    timestamp = [timestamp for i in range(len(metadata['stations']))]
    
    # timestamp = [data['items'][0]['timestamp'] for i in range(len(metadata['stations']))]
    location = [station['location'] for station in metadata['stations']]
    latitude = [station['latitude'] for station in location]
    longitude = [station['longitude'] for station in location]
    name = [station['name'] for station in metadata['stations']]
    station = [station['id'] for station in metadata['stations']]
    reading_type = metadata['reading_type']
    reading_unit = metadata['reading_unit']
    
    df_metadata = pd.DataFrame(
        {
            'timestamp':timestamp,            
            'name':name,
            'latitude':latitude,
            'longitude':longitude,
            'station':station,
            'reading_type':[reading_type for i in range(len(metadata['stations']))],
            'reading_unit':[reading_unit for i in range(len(metadata['stations']))],
            'Description':[measure for i in range(len(metadata['stations']))]
        }
    )        

    df_metadata['timestamp'] = pd.to_datetime(df_metadata['timestamp'])

    project_id="ml-eng-cs611-group-project"
    dataset_id="taxi_dataset"

    item_table = dataset_id+'.'+measure+'-items'
    metadata_table = dataset_id+'.'+measure+'-metadata'
    
    df_items.to_gbq(item_table,project_id,chunksize=None,if_exists='append')

    df_metadata.to_gbq(metadata_table,project_id,chunksize=None,if_exists='append')

    return None