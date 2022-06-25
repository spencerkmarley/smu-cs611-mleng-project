import gcsfs

import pandas as pd
import geopandas as gpd
from scipy.spatial import distance
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore')

from src import assignment

def get_grid_data(gridfile:str) -> type(gpd.GeoDataFrame):
    '''Read the shpfile containing Singapore grid data
    Args:
        gridfile:str:   Path to gridfile
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
    grids_df['grid_num']=grids_df['grid_num'].astype(int)
    return grids_df


def query_nea_metadata(measure:str, query:str, project_id:str='ml-eng-cs611-group-project',dataset_id:str='taxi_dataset_reference')->type(pd.DataFrame):
    '''Query NEA BigQuery for metadata
    Args:
        measure:            rainfall, relative-humidity or air-temperature
        query_timestamp:    i.e. 2022-06-01 13:15:00
        project_id:         Google Cloud project_id
        dataset_id:         Google Cloud dataset_id
    Returns:
        pandas.DataFrame containing metadata for selected measure
    '''
    table_dict={'rainfall':'rainfall-metadata','relative-humidity':'relative-humidity-metadata','air-temperature':'air-temperature-metadata'}

    sql = f"""
    SELECT timestamp, station, latitude, longitude
    FROM `{dataset_id}.{table_dict[measure]}`
    WHERE timestamp = '{query}'
    """

    return pd.read_gbq(sql, project_id=project_id)


def query_nea_items(measure:str, query:str, project_id:str='ml-eng-cs611-group-project',dataset_id:str='taxi_dataset_reference')->type(pd.DataFrame):
    '''Query NEA BigQuery for values
    Args:
        measure:            rainfall, relative-humidity or air-temperature
        query_timestamp:    i.e. 2022-06-01 13:15:00
        project_id:         Google Cloud project_id
        dataset_id:         Google Cloud dataset_id
    Returns:
        pandas.DataFrame containing metadata for selected measure
    '''

    table_dict={'rainfall':'rainfall-items','relative-humidity':'relative-humidity-items','air-temperature':'air-temperature-items'}

    sql = f"""
    SELECT timestamp, station_id, value
    FROM `{dataset_id}.{table_dict[measure]}`
    WHERE timestamp = '{query}'
    """

    return pd.read_gbq(sql, project_id=project_id)


def assign_measure(query:str,measure:str,grids_df:type(gpd.GeoDataFrame))->type(pd.DataFrame):
    '''Generate ranked assignment of stations to grids sorted by Euclidean distance
    Args:
        query:str:                      Timestamp to query databases e.g. '2022-06-01 13:15:00'
        measure:str:                    relative-humidity, rainfall or air-temperature
        grids_df:geopandas.DataFrame:   DataFrame containing map data of Singapore
    
    Returns:
        measure_df  pandas DataFrame of the following schema:
            grid_num:       Unique integer representing a grid in grids_df
            date_active:    Date which mapping is valid
            date_inactive:  Date which mapping becomes invalid
            rank:           Integer declaring position relating to distance of station_id from grid_num
            station_id:     Unique identifier for station            
    '''

    df_metadata=query_nea_metadata(measure=measure,query=query)
    df_metadata['latlon']=df_metadata[['longitude','latitude',]].apply(tuple,axis=1)
    df_metadata.index=df_metadata['station']
    grid_nums = list(grids_df['grid_num'].unique())
    assignment = {}

    for i in tqdm(range(len(grid_nums)),desc='Grids assigned'):
        grid_coordinates = grids_df.iloc[i]['latlon'] # latlon of row i grid_num    
        distances = df_metadata['latlon'].apply(lambda x: distance.euclidean(x,grid_coordinates))
        distance_sorted = distances.sort_values()
        ranked={k:v for k,v in enumerate(distance_sorted.index.values)}
        assignment[grids_df.iloc[i]['grid_num']]=ranked

    measure_df=pd.DataFrame(assignment).T.reset_index().melt(id_vars='index')
    measure_df.rename(columns={'index':'grid_num','variable':'rank','value':'station_id'},inplace=True)    
    measure_df['date_active']=pd.to_datetime(query)
    measure_df['date_inactive']=pd.to_datetime('2050-12-31 00:00:00')
    measure_df['rank']=measure_df['rank'].astype(int)
    measure_df = measure_df[['grid_num','date_active','date_inactive','rank','station_id']]
    return measure_df


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Ranks stations by Euclidiean distance from the centroid of each grid for the purposes of assigning a weather station to a grid')
    parser.add_argument('--project','-p', nargs='?', default='ml-eng-cs611-group-project', type=str, help='GCP project name i.e. ml-eng-cs611-group-project')
    parser.add_argument('--bucket','-b', nargs='?', default='ml-eng-cs611-group-project-nea', type=str, help='GCS bucket name i.e. ml-eng-cs611-group-project-nea')
    parser.add_argument('--dataset_id','-d', nargs='?', default='taxi_dataset_reference', type=str, help='GBQ dataset ID i.e. taxi_dataset for live data, taxi_dataset_reference for reference data.')
    parser.add_argument('--table_id','-t', nargs='?', default='assignment-station', type=str, help='GBQ table ID i.e. assignment-taxi')
    parser.add_argument('--gridfile','-g', nargs='?', default='./updated codes/filter_grids_2/filter_grids_2.shp', type=str, help='Path to gridfiles (static data)')
    parser.add_argument('--query','-q', type=str, help='Timestamp to assign i.e. 2022-06-01 13:15:00')
    args = parser.parse_args()
    
    project = args.project
    bucket = args.bucket
    dataset_id = args.dataset_id
    table_id = args.table_id
    gridfile = args.gridfile
    query = args.query

    measures = ['rainfall','air-temperature','relative-humidity']

    fs = gcsfs.GCSFileSystem(project=project)
        
    ### Section 1: Read grid file data and preprocess
    grids_df = get_grid_data(gridfile)
    
    # Get unique grid_num
    grid_nums = list(grids_df['grid_num'].unique())

    ### Section 2: Assign NEA stations to grid
    
    for measure in measures:
        print(f"Assigning stations for [{measure}]")
        result=assign_measure(query,measure,grids_df)    
        table_id=f'assignment-station-{measure}'
        print(f"Writing to table {dataset_id}.{table_id}")
        result.to_gbq('.'.join([dataset_id,table_id]),project,if_exists='append')