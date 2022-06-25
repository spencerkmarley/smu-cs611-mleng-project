import gcsfs

import pandas as pd
import geopandas as gpd
from scipy.spatial import distance
from tqdm import tqdm

import math
from datetime import datetime,timedelta
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

def query_taxi_availability(query:str, project_id:str='ml-eng-cs611-group-project',dataset_id:str='taxi_dataset_reference')->type(pd.DataFrame):
    '''Query LTA BigQuery for taxi coordinates
    Args:        
        query_timestamp:    i.e. 2022-06-01 13:15:00
        project_id:         Google Cloud project_id
        dataset_id:         Google Cloud dataset_id
    Returns:
        pandas.DataFrame containing metadata for taxi data of the schema
            - timestamp:datetime.timestamp  Timestamp of observation for a taxi
            - longitude:float               Longitude of a taxi in degrees
            - latitude:float                Latitude of a taxi in degrees
    '''
    sql = f"""
    SELECT *
    FROM `{dataset_id}.taxi-availability`
    WHERE timestamp = '{query}'
    """

    return pd.read_gbq(sql, project_id=project_id)


def get_grid_data(gridfile:str)->type(gpd.GeoDataFrame):
    '''Read the shpfile containing Singapore grid data
    Args:
        gridfile:str:   Path to gridfile
    Returns
        geopandas.DataFrame with following schema
        - grid_num:int                  Unique grid ID in Singapore 
        - intersect                     
        - geometry (active geometry)    shapely Objects describing the grid
        - latlon (tuple)                Tuple of coordinates of the form (longitude,latitude)
    '''
    grids = gpd.read_file(gridfile)
    grids['centroid'] = grids['geometry'].apply(lambda x: x.centroid) # get grids' centroid

    # convert to dataframe
    grids_df = pd.DataFrame(grids)
    grids_df['centroid'] = grids_df['centroid'].astype(str)
    grids_df['latlon'] = grids_df['centroid'].apply(lambda x: (float(x.split(' ')[1][1:]), float(x.split(' ')[2][:-1])))
    return grids_df


def assign_taxis(taxi_df:type(pd.DataFrame),grids)->type(pd.DataFrame):
    '''Numerically determine grid number of each taxi, and counts the number of taxis in each grid
    Args:
        taxi_df:pandas.DataFrame
        grids:geopandas.DataFrame
    Yields:
        taxi_clean:pandas.DataFrame containing metadata for taxi data of the schema
            - timestamp:datetime.timestamp  Timestamp for assignment
            - grid_num:int                  Unique grid in Singapore GeoDataFrame
            - taxi_count:float              Number of taxis in grid
    '''
    def get_grid_longitude(longitude:float):
        '''Maps longitude to a grid'''
        return math.ceil((longitude-103.6)/0.020454545454545583)


    def get_grid_latitude(latitude:float):
        '''Maps latitude to a grid'''
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
    return taxi_clean


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Generates ')
    parser.add_argument('--project','-p', nargs='?', default='ml-eng-cs611-group-project', type=str, help='GCP project name i.e. ml-eng-cs611-group-project')    
    parser.add_argument('--dataset_id','-d', nargs='?', default='taxi_dataset_reference', type=str, help='GBQ dataset ID i.e. taxi_dataset. For live data, use taxi_dataset')
    parser.add_argument('--table_id','-t', nargs='?', default='assignment-taxi', type=str, help='GBQ table ID i.e. assignment-taxi')
    parser.add_argument('--gridfile','-g', nargs='?', default='./updated codes/filter_grids_2/filter_grids_2.shp', type=str, help='Path to gridfiles (static data)')    
    parser.add_argument('--query','-q', type=str, help='Timestamp to start assignment i.e. 2022-06-01 13:15:00')
    parser.add_argument('--batch','-B', nargs='?', default=0, type=int, help='No. of 15 min intervals to batch process')
    args = parser.parse_args()
    
    project = args.project    
    dataset_id = args.dataset_id
    table_id = args.table_id
    gridfile = args.gridfile    
    query = args.query
    batch = args.batch

    print(f"Assigning taxi data for timestamp {query} and {batch} more batch(es)")

    fs = gcsfs.GCSFileSystem(project=project)
        
    ### Read grid file data and preprocess
    grids_df = get_grid_data(gridfile)
    grid_nums = list(grids_df['grid_num'].unique())    

    ### Assign taxis to grid
    query_datetime = datetime.strptime(query,'%Y-%m-%d %H:%M:%S')

    date_list=[query_datetime + timedelta(minutes=15*x) for x in range(1+batch)]
    df_list=[]
    for date in tqdm(date_list):
        taxi = query_taxi_availability(date)
        try:
            taxi_df = assign_taxis(taxi,grids_df)
            df_list.append(taxi_df)
        except:
            print(f"Empty data for {date}")
    consolidated_df = pd.concat(df_list)

    ### Write to BigQuery
    consolidated_df.to_gbq('.'.join([dataset_id,table_id]),project,if_exists='append')
    print(f"Loaded {1+batch} timestamp(s) successfully")