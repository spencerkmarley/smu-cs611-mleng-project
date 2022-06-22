
import pandas as pd
import gcsfs

from scipy.spatial import distance
from datetime import datetime

import math
from collections import Counter

from tqdm import tqdm

import geopandas as gpd

from src import assignment

import warnings
warnings.filterwarnings('ignore')

def query_taxi_availability(query:str, project_id='ml-eng-cs611-group-project',dataset_id='taxi_dataset_reference'):
    '''Query LTA BigQuery for taxi coordinates
    Args:        
        query_timestamp:    i.e. 2022-06-01 13:15:00
        project_id:         Google Cloud project_id
        dataset_id:         Google Cloud dataset_id
    Returns:
        pandas.DataFrame containing metadata for taxi data
    '''
    sql = f"""
    SELECT *
    FROM `{dataset_id}.taxi-availability`
    WHERE timestamp = '{query}'
    """

    return pd.read_gbq(sql, project_id=project_id)


def get_grid_data(gridfile:str):
    '''Read the shpfile containing Singpaore grid data
    Args:
        gridfile:str:Path to gridfile
    Returns
        geopandas.DataFrame with following schema
        - grid_num 
        - intersect
        - geometry (active geometry)        
        - latlon (tuple)
    '''
    grids = gpd.read_file(gridfile)
    grids['centroid'] = grids['geometry'].apply(lambda x: x.centroid) # get grids' centroid

    # convert to dataframe
    grids_df = pd.DataFrame(grids)
    grids_df['centroid'] = grids_df['centroid'].astype(str)
    grids_df['latlon'] = grids_df['centroid'].apply(lambda x: (float(x.split(' ')[1][1:]), float(x.split(' ')[2][:-1])))
    return grids_df


def assign_taxis(taxi_df,grids):
    '''Numerically determine grid number of each taxi, and counts the number of taxis in each grid
    Args:
        taxi_df:pandas.DataFrame
        grids:geopandas.DataFrame
    Yields:
        taxi_clean:pandas.DataFrame
    '''
    def get_grid_longitude(longitude:float):    
        return math.ceil((longitude-103.6)/0.020454545454545583)


    def get_grid_latitude(latitude:float):
        return (13 - math.ceil((latitude -1.208)/0.020538461538461547))*22

    longitude_array = taxi_df['longitude'].to_numpy()
    latitude_array = taxi_df['latitude'].to_numpy()

    test = [get_grid_longitude(longitude_array[i]) + get_grid_latitude(latitude_array[i]) for i in range(len(longitude_array))]

    # getting dictionary of items
    c = Counter(test)

    # Getting taxi_count for relevant grid_num
    df_taxicount = pd.DataFrame({'grid_num': [float(x) for x in list(c.keys())], 
                                 'taxi_count': [x[1] for x in list(c.items())]})

    # Get full list of grid_num as a dataframe:  grid_num | timestamp
    all_grids = grids[['grid_num']]
    all_grids['timestamp'] = taxi_df['timestamp'][0]

    # Merge all_grids and df_taxicount
    taxi_clean = pd.merge(all_grids, df_taxicount, how='left')
    taxi_clean['taxi_count'] = taxi_clean['taxi_count'].fillna(0) #fill missing taxi_count = 0
    taxi_clean['grid_num']=taxi_clean['grid_num'].apply(int)    
    taxi_clean = taxi_clean.set_index('grid_num')
    taxi_clean.drop('timestamp',axis=1,inplace=True)
    return taxi_clean


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Generates ')
    parser.add_argument('--project','-p', nargs='?', default='ml-eng-cs611-group-project', type=str, help='GCP project name i.e. ml-eng-cs611-group-project')
    parser.add_argument('--bucket','-b', nargs='?', default='ml-eng-cs611-group-project-nea', type=str, help='GCS bucket name i.e. ml-eng-cs611-group-project-nea')
    parser.add_argument('--dataset_id','-d', nargs='?', default='taxi_dataset_reference', type=str, help='GBQ dataset ID i.e. taxi_dataset')
    parser.add_argument('--table_id','-t', nargs='?', default='assignment-taxi', type=str, help='GBQ table ID i.e. assignment-taxi')
    parser.add_argument('--gridfile','-g', nargs='?', default='./updated codes/filter_grids_2/filter_grids_2.shp', type=str, help='Path to gridfiles (static data)')    
    parser.add_argument('--query','-q', type=str, help='Timestamp to assign i.e. 2022-06-01 13:15:00')    
    args = parser.parse_args()
    
    project = args.project
    bucket = args.bucket
    dataset_id = args.dataset_id
    table_id = args.table_id
    gridfile = args.gridfile    
    query = args.query

    fs = gcsfs.GCSFileSystem(project=project)
        
    ### Section 1: Read grid file data and preprocess
    grids_df = get_grid_data(gridfile)
    
    # Get unique grid_num
    grid_nums = list(grids_df['grid_num'].unique())    

    ### Section 3: Assign taxis to grid

    taxi = query_taxi_availability(query)
    taxi_df = assign_taxis(taxi,grids_df)   

    taxi_df.to_gbq('.'.join([dataset_id,table_id]),project,if_exists='append')