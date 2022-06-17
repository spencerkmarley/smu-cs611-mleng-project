import pandas as pd
import gcsfs
import sys
import json
import gcsfs
import datetime
import geopandas as gpd
from shapely import wkt
from shapely.ops import nearest_points
import warnings
import math
from matplotlib import pyplot as plt
import seaborn as sns
import itertools
from datetime import date, timedelta
from tqdm import tqdm
import joblib
import re
from collections import Counter
warnings.filterwarnings('ignore')
sys.path.append('smu-cs611-mleng-project')
from src import jsonParser

class Assignment():
    '''Object to assign taxi and NEA data to grids
    Args:
        grids: Pre-generated shapefile with grids
        nea_data(dict): Dictionary containing the three measures
        taxi_data(df)
    '''
    def __init__(self,grids,nea_data,taxi_data):
        self.nea_data=nea_data
        self.grids=grids
        self.taxi_data=taxi_data
        self.taxi_timestamp=taxi_data['timestamp'][0]
        self.timestamp={
            'relative-humidity':self.nea_data['relative-humidity']['metadata']['timestamp'][0],
            'air-temperature':self.nea_data['air-temperature']['metadata']['timestamp'][0],
            'rainfall':self.nea_data['rainfall']['metadata']['timestamp'][0]
        }
    
    def nea_preprocess(self):
        '''Sequentially determines which weather station covers each grid in the shapefile
        '''
        def convert_gpd(df):
            '''
            '''
            coord = list(zip(df['longitude'], df['latitude']))
            iterim = df
            iterim["Coordinates"] = [f'POINT({str(i[0])} {str(i[1])})' for i in coord]

            iterim['geometry'] = iterim.Coordinates.apply(wkt.loads)
            gdf_stn = gpd.GeoDataFrame(iterim, geometry='geometry')
            gdf_stn.drop('Coordinates', inplace=True, axis=1)
            gdf_stn.reset_index(inplace=True)

            return gdf_stn
        
        def near(point, gdf, pts):
            # find the nearest point and return the corresponding Place value
            nearest = gdf.geometry == nearest_points(point, pts)[1]
            return gdf[nearest].station.to_numpy()[0]
        
        measures = ['rainfall','air-temperature','relative-humidity']
        
        for measure in measures:
            print(f"Now preprocessing for [{measure}]")
            items = self.nea_data[measure]['items']
            metadata = self.nea_data[measure]['metadata']
            timestamp = self.timestamp[measure]
            
            val = items[items["timestamp"] == timestamp]
            ss = metadata[metadata['timestamp'] == timestamp]
            gdf_stn = convert_gpd(ss)
            
            pts3 = gdf_stn.geometry.unary_union
            
            print(f"{measure} pts has {len(pts3)} points")
            
            self.grids['station_id'] = self.grids.apply(lambda row: near(row.geometry, gdf_stn, pts3), axis=1)
            self.grids = self.grids.merge(val, on = ['station_id'], how='inner')
            self.grids.rename(columns = {'value':items['Description'][0],'station_id':f'{measure}_station_id'}, inplace = True)
            self.grids = self.grids.drop(['timestamp'], axis=1)
            self.grids['grid_num']=self.grids['grid_num'].astype(int)
        
        self.grids=self.grids.sort_values('grid_num')
        
        return self.grids
    
    def taxi_preprocess(self):        
        try:
            one_list = [(i.x,i.y) for i in self.taxi_data['geometry']]
            print(f"[{len(one_list)}] coordinates parsed")
            
            ## Getting list of grid num
            test = [math.ceil((i[0]-103.6)/0.01) + math.ceil(27 - (i[1] -1.208)/0.009888890000000039)*45 for i in one_list]
            print(f"test array is length [{len(test)}]")
            
            # getting dictionary of items
            c = Counter(test)

            # Getting taxi_count for relevant grid_num
            self.taxi_count = pd.DataFrame({'timestamp':[self.taxi_data['timestamp'][0] for x in range(len(c))] ,'grid_num': list(c.keys()), 
                                         'taxi_count': [x[1] for x in list(c.items())]}).sort_values('grid_num')
            
        except:
            print("Taxi assignment failed")
            self.taxi_count = None
            pass
        
        return self.taxi_count
    
    def merge_grids(self):
        self.grids = self.grids.merge(self.taxi_count, how='left',on='grid_num')
        self.grids['taxi_count']=self.grids['taxi_count'].fillna(0)
        self.grids['timestamp']=self.grids['timestamp'].fillna(self.taxi_timestamp)
        return self.grids