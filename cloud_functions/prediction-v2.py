import functions_framework
import math
import datetime
import pandas as pd
import numpy as np
from google.cloud import aiplatform
from absl import logging
import requests

# requirements.txt
# functions-framework==3.*
# google-cloud-aiplatform
# pandas
# pandas-gbq
# absl-py
# requests

def get_grid_longitude(longitude:float):
    '''Maps longitude to a grid'''
    return math.ceil((longitude-103.6)/0.020454545454545583)


def get_grid_latitude(latitude:float):
    '''Maps latitude to a grid'''
    return (13 - math.ceil((latitude -1.208)/0.020538461538461547))*22

def round_time(dt=None, date_delta=datetime.timedelta(minutes=1), to='average'):
    """
    Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    round_to = date_delta.total_seconds()
    if dt is None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds

    if seconds % round_to == 0 and dt.microsecond == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == 'up':
            # // is a floor division, not a comment on following line (like in javascript):
            rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + datetime.timedelta(0, rounding - seconds, - dt.microsecond)

def onemap_query(address):
    latlng = {"lat":[],"lng":[]}
    req = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address='+address+'&key=AIzaSyAx-PZe_4zUoUttV5vcfwsbQCqD78k9ZyQ')
    resultsdict = req.json()
    lat = resultsdict['results'][0]['geometry']['location']['lat']
    latlng["lat"].append(lat) 
    lng = resultsdict['results'][0]['geometry']['location']['lng']
    latlng["lng"].append(lng)
    return latlng

@functions_framework.http
def hello_world(request):
    """Responds to any HTTP request. Takes Dialogflow webhook request and parses input for prediction.
    """

 
    request_json = request.get_json()
    print(f"Dialogflow received {request_json}")

    GOOGLE_CLOUD_PROJECT = 'ml-eng-cs611-group-project'     
    GOOGLE_CLOUD_REGION = 'asia-southeast1'
    ENDPOINT_ID='1152815951490580480'
    
    # Parse Dialogflow JSON
    intent = request_json['queryResult']['action']
   
    if intent == 'Prediction.Prediction-next':
        print("Parsing user coordinates")
        longitude = float(request_json['queryResult']['outputContexts'][0]['parameters']['longitude'][0])
        latitude = float(request_json['queryResult']['outputContexts'][0]['parameters']['latitude'][0])

    else:
        address_dict = request_json['queryResult']['parameters']['street-address']
        address = ''.join([address_dict[i] for i in address_dict.keys()])+', Singapore'
        response_json=onemap_query(address)
        latitude = response_json['lat'][0]
        longitude = response_json['lng'][0]

    print(f"Lat:{latitude} Long:{longitude}")

    # Calculate grid
    grid_num = get_grid_longitude(longitude) + get_grid_latitude(latitude)

    # Calculate timestamp to look up
    ct = datetime.datetime.now()
    rounded_timestamp = round_time(dt=ct,date_delta=datetime.timedelta(minutes=15),to='down') + datetime.timedelta(hours=8)
    print(f"rounded_timestamp is {rounded_timestamp}")
    
    # Query BigQuery for weather data
    project_id='ml-eng-cs611-group-project'
    dataset_id='taxi_dataset_views'
    table_id='all-item-join-live'    

    query = f"""SELECT * FROM `{dataset_id}.{table_id}`
    WHERE timestamp = '{rounded_timestamp}'
    """

    weather_data=pd.read_gbq(query,project_id=project_id)

    if grid_num not in weather_data['grid_num'].values:
        result='Invalid coordinates received. Cannot map to grid. Please ensure your coordinates are within Singapore borders.'

    else:
    # Get hour, month, day
        weather_data['hour'] = weather_data['timestamp'].apply(lambda x: x.hour)
        weather_data['month'] = weather_data['timestamp'].apply(lambda x: x.month)
        weather_data['day'] = weather_data['timestamp'].apply(lambda x: x.weekday())

        weather_data['sin_day'] = weather_data['day'].apply(lambda x: math.sin(x/7*2*math.pi)) # get sin for day
        weather_data['cos_day'] = weather_data['day'].apply(lambda x: math.cos(x/7*2*math.pi)) # get sin for day
        weather_data['sin_hour'] = weather_data['hour'].apply(lambda x: math.sin(x/24*2*math.pi)) # get sin for hour
        weather_data['cos_hour'] = weather_data['hour'].apply(lambda x: math.cos(x/24*2*math.pi)) # get sin for hour
        weather_data['sin_mth'] = weather_data['month'].apply(lambda x: math.sin(x/12*2*math.pi)) # get sin for mth
        weather_data['cos_mth'] = weather_data['month'].apply(lambda x: math.cos(x/12*2*math.pi)) # get sin for mth

        weather_data = weather_data.drop(columns=['hour', 'month', 'day'])
        # Format model input
        model_input = weather_data[weather_data['grid_num']==grid_num]
        print(f"The array going into model is {model_input}")
        # Get Predictions

        if not ENDPOINT_ID:
            from absl import logging
            logging.error('Please set the endpoint id.')

        client_options = {
            'api_endpoint': GOOGLE_CLOUD_REGION + '-aiplatform.googleapis.com'
            }
        # Initialize client that will be used to create and send requests.
        client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)

        # Set data values for the prediction request.
        # Our model expects 4 feature inputs and produces 3 output values for each
        # species. Note that the output is logit value rather than probabilities.
        # See the model code to understand input / output structure.

        instances = [{            
            "taxi_count":model_input['taxi_count'].astype('float').to_list(), 
            "air_temperature":model_input['air_temperature'].to_list(), 
            "rainfall": model_input['rainfall'].to_list(), 
            "relative_humidity":model_input['relative_humidity'].to_list(), 
            "sin_day":model_input['sin_day'].to_list(), 
            "cos_day":model_input['cos_day'].to_list(), 
            "sin_hour":model_input['sin_hour'].to_list(), 
            "cos_hour":model_input['cos_hour'].to_list(), 
            "sin_mth":model_input['sin_mth'].to_list(), 
            "cos_mth":model_input['cos_mth'].to_list()
        }]

        endpoint = client.endpoint_path(
            project=GOOGLE_CLOUD_PROJECT,
            location=GOOGLE_CLOUD_REGION,
            endpoint=ENDPOINT_ID,
        )
        # Send a prediction request and get response.
        response = client.predict(endpoint=endpoint, instances=instances)
        predicted_taxis=int(response.predictions[0][0])
        current_taxis=model_input['taxi_count'].to_list()[0]
        if predicted_taxis>10:
            availability='Good'
        elif predicted_taxis>5:
            availability='Fair'
        else:
            availability='Poor'

        if predicted_taxis > current_taxis and current_taxis<=5:
            recommendation=f'Book later as taxi availability will improve by [{100*(predicted_taxis/current_taxis-1):.2f}]%'
        elif predicted_taxis < current_taxis:
            recommendation=f'Book now as taxi availability may worsen by [{100*(predicted_taxis/current_taxis-1):.2f}]%'
        else:
            recommendation='Book now as difference may not be substantial'

        result = f'Predicted taxis in 30 minutes is [{predicted_taxis}]: Availability is [{availability}]. Recommendation: [{recommendation}]'

    fulfilment = {
        "fulfillmentMessages": [
            {
                "text": {
                    "text": [
                        result
                        ]
                        }
                        }
                        ]
                        }
    return fulfilment