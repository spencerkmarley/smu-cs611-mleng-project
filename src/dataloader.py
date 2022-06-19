from pydantic import NoneStr
import requests
import numpy as np
import pandas as pd
import itertools
import re
import argparse
import json

from sklearn import datasets

def get_weather_data(query:str,dataset:str=None):
    '''Returns dictionary of JSON objects for weather data given a datetime string
    Each entry in the dictionary is the JSON response for each of the API Endpoints of
     ["air-temperature","rainfall","relative-humidity"]    
    '''
    if dataset:
        data_sets=[dataset]
    else:
        data_sets = ["air-temperature","rainfall","relative-humidity"]

    results={}

    for measure in data_sets:
        URL = "https://api.data.gov.sg/v1/environment/"+measure
        params={'date_time':query}
        r=requests.get(URL,params=params)
        results[measure]=r.json()
    return results

def get_taxi_data(query:str):
    '''Returns the coordinates of all taxis via the LTA API endpoint for a given datetime string
    '''
    URL = "https://api.data.gov.sg/v1/transport/taxi-availability"
    params={'date_time':query}
    r=requests.get(URL,params=params)
    return r.json()

def assign_taxis(taxi_array:type(np.array),station_array:type(np.array)):
    cartesian_prod = itertools.product(taxi_array,station_array) # Cartesian product for taxi and weather station coordinates
    diff = np.array([i[0]-i[1] for i in cartesian_prod]) 
    distance = np.linalg.norm(diff,axis=1)
    distance_matrix = distance.reshape((len(taxi_array),len(station_array),-1)) # Reshape back into distances since the cartesian product only has one dimension
    return [np.argmin(distance_matrix[i]) for i in range(len(taxi_array))]

def generate_dataset(timestamps:list):
    '''Fetches taxi and weather data for multiple timestamps and returns a dataframe of the data
    '''    

    results=pd.DataFrame(columns=['device_id','latitude','longitude','rainfall','taxis'])

    for timestamp in timestamps:

        weather_data=get_weather_data(query=timestamp)
        taxi_data=get_taxi_data(query=timestamp)

        taxi_coordinates = taxi_data['features'][0]['geometry']['coordinates']
        station_dict = weather_data['rainfall']['metadata']['stations']
        taxi_array = np.array([t[::-1] for t in taxi_coordinates]) # taxi_coordinates are reversed, need to fix
        station_array = np.array([[station_dict[i]['location']['latitude'],station_dict[i]['location']['longitude']] for i in range(len(station_dict))])

        assignment = assign_taxis(taxi_array,station_array)
        assignment_series = pd.Series(assignment)
        assignment_counts = assignment_series.value_counts().sort_index().rename('taxis')

        station_id = [i['id'] for i in station_dict]
        rainfall_array = np.array([i['value'] for i in weather_data['rainfall']['items'][0]['readings']])        

        lat_series = pd.Series(station_array[:,0],name='latitude')
        lon_series = pd.Series(station_array[:,1],name='longitude')
        rainfall_series = pd.Series(rainfall_array,name='rainfall')
        station_id_series = pd.Series(station_id,name='device_id')

        df = pd.concat([station_id_series, lat_series,lon_series,rainfall_series,assignment_counts],axis=1)
        df['timestamp']=query
        results = pd.concat([results,df])
    
    return results
    
if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Queries APIs and downloads JSON file')    
    parser.add_argument('--measure','-m', nargs='?',type=str, help='NEA measure or Taxi i.e.: rainfall, air-temperature, relative-humidity, taxi-availability')
    parser.add_argument('--query','-q', nargs=1,type=str, help='timestamp to query API')
    
    args = parser.parse_args()
    query=args.query[0] # For some reason it parses as a list.
    measure = args.measure

    if measure == 'taxi-availability':
        results = get_taxi_data(query)
    else:
        if measure:
            results = get_weather_data(query,measure)
        else:
            results = get_weather_data(query)

    query = query.replace(':','-')

    for measure in results.keys():
        with open(f"{measure}_{query}.json", "w") as outfile:
            json.dump(results[measure], outfile)