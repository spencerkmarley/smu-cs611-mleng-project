import google.cloud.storage as storage
import functions_framework

import pandas as pd
import geopandas as gpd
from shapely import wkt

from datetime import datetime
import json
import re
import math
from collections import Counter

# requirements.txt
# functions-framework==3.*
# google.cloud.storage
# pandas
# pandas-gbq
# geopandas
# shapely

def jsonParserTaxi(event, context):  
  """Triggered by a change to a Cloud Storage bucket. Assigns taxis to grid numerically.
  Args:
    event (dict): Event payload.
    context (google.cloud.functions.Context): Metadata for the event.
  """

  def get_grid_longitude(longitude:float):
    '''Maps longitude to a grid'''
    return math.ceil((longitude-103.6)/0.020454545454545583)

  def get_grid_latitude(latitude:float):
    '''Maps latitude to a grid'''
    return (13 - math.ceil((latitude -1.208)/0.020538461538461547))*22

  dataset_id = 'taxi_dataset'
  project = "ml-eng-cs611-group-project"
  bucket = "ml-eng-cs611-group-project-taxis"
  table_id = 'taxi-availability'
  table_id_assignment = 'assignment-taxi'

  client = storage.Client(project=project)
  bucket = client.get_bucket(bucket)

  print(f"event is {event}")
  print(f"context is {context}")

  # Parse LTA JSON files
  path = event['name']
  blob = bucket.blob(path)
  lta_data = json.loads(blob.download_as_string(client=None))  
  features = lta_data['features'][0]
  coordinates = features['geometry']['coordinates']
  longitude = [i[0] for i in coordinates]
  latitude = [i[1] for i in coordinates]
  
  pattern = r"\d\d\d\d-\d\d-\d\dT\d\d-\d\d-\d\d"
  re_timestamp = re.compile(pattern)
  timestamp = re_timestamp.findall(path)[0]
  timestamp = datetime.strptime(timestamp[:16], '%Y-%m-%dT%H-%M')
  timestamp = [timestamp for i in range(len(coordinates))]
  df = pd.DataFrame({'timestamp':timestamp,'longitude':longitude,'latitude':latitude})
  df['timestamp'] = pd.to_datetime(df['timestamp'])
  df['timestamp'] = df['timestamp'].dt.tz_localize(None)
  print(f"Writing taxi data to {dataset_id}.taxi-availability")
  df.to_gbq(dataset_id+'.'+'taxi-availability',project,chunksize=None,if_exists='append')

  longitude_array = df['longitude'].to_numpy()
  latitude_array = df['latitude'].to_numpy()

  test = [get_grid_longitude(longitude_array[i]) + get_grid_latitude(latitude_array[i]) for i in range(len(longitude_array))]

  # # getting dictionary of items
  c = Counter(test)

  # # Getting taxi_count for relevant grid_num
  df_taxicount = pd.DataFrame(
    {
      'timestamp':[timestamp[0] for x in list(c.keys())],
      'grid_num': [int(x) for x in list(c.keys())],
      'taxi_count': [float(x[1]) for x in list(c.items())]
    }                                
  )

  df_taxicount['taxi_count'] = df_taxicount['taxi_count'].fillna(0) #fill missing taxi_count = 0   
  df_taxicount.to_gbq('.'.join([dataset_id,table_id_assignment]),project,if_exists='append')