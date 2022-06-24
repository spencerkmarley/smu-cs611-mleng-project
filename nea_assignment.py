
import pandas as pd
import gcsfs

from scipy.spatial import distance

from tqdm import tqdm

import geopandas as gpd

from src import assignment

import warnings
warnings.filterwarnings('ignore')

def query_nea_metadata(measure:str, query:str, project_id='ml-eng-cs611-group-project',dataset_id='taxi_dataset_reference'):
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


def query_nea_items(measure:str, query:str, project_id='ml-eng-cs611-group-project',dataset_id='taxi_dataset_reference'):
    '''Query NEA BigQuery for metadata
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


def query_nea_view(measure:str, query:str, project_id='ml-eng-cs611-group-project',dataset_id='taxi_dataset_views'):
    '''Query NEA BigQuery for metadata
    Args:
        measure:            rainfall, relative-humidity or air-temperature
        query_timestamp:    i.e. 2022-06-01 13:15:00
        project_id:         Google Cloud project_id
        dataset_id:         Google Cloud dataset_id
    Returns:
        pandas.DataFrame containing metadata for selected measure
    '''
    table_dict={'rainfall':'view-rainfall','relative-humidity':'view-relative-humidity','air-temperature':'view-air-temperature'}

    sql = f"""
    SELECT *
    FROM `{dataset_id}.{table_dict[measure]}`
    WHERE timestamp = '{query}'
    """

    return pd.read_gbq(sql, project_id=project_id)


def assign_grids(grids_df, df_metadata, df_items, grid_nums):
    '''
    Arguments
    grids_df: dataframe that contains grid numbers and their centroid latlon    
    df_metadata: dataframe with station metadata i.e. latitude and longitude
    df_: dataframe that contains stn latlon
    ts: timestamp
    grid_nums: list of unique grid numbers
    '''
    
    df_metadata['latlon']=df_metadata[['longitude','latitude',]].apply(tuple,axis=1)
    df_metadata.index=df_metadata['station']
    assignment={}

    for i in tqdm(range(len(grid_nums)),desc='Grids assigned'): # for each grid_num
        grid_coordinates = grids_df.iloc[i]['latlon'] # latlon of row i grid_num
        
        distances = df_metadata['latlon'].apply(lambda x: distance.euclidean(x,grid_coordinates))
        distance_sorted = distances.sort_values()
        # print(distance_sorted)
        for station in distance_sorted.index:
            if len(df_items[df_items['station_id']==station])>0: # there is a value
                # print(f"Assigning station {station} to grid {i}")
                assignment[grids_df.iloc[i]['grid_num']]=station
                break
        
            else:
                continue           
    
    return assignment


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
    grids_df['grid_num']=grids_df['grid_num'].astype(int)
    return grids_df


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Generates ')
    parser.add_argument('--project','-p', nargs='?', default='ml-eng-cs611-group-project', type=str, help='GCP project name i.e. ml-eng-cs611-group-project')
    parser.add_argument('--bucket','-b', nargs='?', default='ml-eng-cs611-group-project-nea', type=str, help='GCS bucket name i.e. ml-eng-cs611-group-project-nea')
    parser.add_argument('--dataset_id','-d', nargs='?', default='taxi_dataset_reference', type=str, help='GBQ dataset ID i.e. taxi_dataset')
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
    assignment = {measure:[] for measure in measures}
    for measure in measures:
        print(f"Assigning {measure} to grid")
        df_metadata=query_nea_metadata(measure=measure,query=query)
        df_items=query_nea_items(measure=measure,query=query)
        result=assign_grids(grids_df,df_metadata,df_items,grid_nums)
        assignment[measure]=result

    assignment_df=pd.DataFrame(assignment)
    assignment_df['date_active']=pd.to_datetime(query)
    assignment_df['date_inactive']=pd.to_datetime('2050-12-31 00:00:00')
    assignment_df=assignment_df[['date_active','date_inactive','rainfall','relative-humidity','air-temperature']]
    assignment_df.reset_index(inplace=True)
    assignment_df.rename(columns={'index':'grid_num','relative-humidity':'relative_humidity','air-temperature':'air_temperature'},inplace=True)

    ### Section 3: Write to Bigquery
    print("Uploading results to Bigquery...")
    assignment_df.to_gbq('.'.join([dataset_id,table_id]),project,if_exists='append')