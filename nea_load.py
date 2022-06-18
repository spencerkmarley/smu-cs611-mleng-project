import gcsfs
import re

from matplotlib.pyplot import get

from src import jsonParser

def load_nea_to_gbq(project:str,dataset_id:str,measure:str,filename:str):
    '''Load a single json 
    Args:
        project:str:        
        dataset_id:str:
        measure:str:
        filename:str:
    '''
    print(f"Current processing {filename}")
    parser = jsonParser.jsonParser(fs)
    
    items = parser.get_items(filename,measure)
    metadata = parser.get_metadata(filename,measure)

    item_table = dataset_id+'.'+measure+'-items'
    metadata_table = dataset_id+'.'+measure+'-metadata'

    print(f"Writing {measure} items to {item_table}")
    items.to_gbq(item_table,project,chunksize=None,if_exists='append')

    print(f"Writing {measure} metadata to {metadata_table}")    
    metadata.to_gbq(metadata_table,project,chunksize=None,if_exists='append')

def date_check(date):
    '''
    Args:
        date(str): Date string to check
    Returns:
        result(bool): Whether its formatted correctly
        
    '''
    pattern = r'\d{4}-\d{2}-\d{2}$'
    date_match = re.compile(pattern)
    result = date_match.findall(date)
    return result

def get_start_index(start_date:str,file_list:list):
    start_index=0

    if date_check(start_date):
        file_re = re.compile(start_date)
        
        for file in file_list:
            if file_re.findall(file):
                start_index=file_list.index(file)
                print(f"Valid start date provided, starting batch loading at index {start_index}")
                break

        return start_index    

    else:
        print("Invalid start date (YYYY-MM-DD) provided. Starting from index 0")
        return start_index

def get_end_index(end_date:str,file_list:list):
    end_index=len(file_list)
    if date_check(end_date):
        file_re = re.compile(end_date)
        for file in file_list[::-1]:
            if file_re.findall(file):
                end_index=file_list.index(file)
                print(f"Valid end date provided, end batch loading at index {end_index}")
                break
        
        return end_index
    else:
        print("Invalid end date (YYYY-MM-DD) provided. Loading all data")
        return end_index

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Reads NEA JSON file to GBQ')
    parser.add_argument('--project','-p', nargs='?', default='ml-eng-cs611-group-project', type=str, help='GCP project name i.e. ml-eng-cs611-group-project')
    parser.add_argument('--bucket','-b', nargs='?', default='ml-eng-cs611-group-project-nea', type=str, help='GCS bucket name i.e. ml-eng-cs611-group-project-nea')
    parser.add_argument('--dataset_id','-d', nargs='?', default='taxi_dataset', type=str, help='GBQ dataset ID i.e. taxi_dataset')
    parser.add_argument('--measure','-m', nargs='?',type=str, help='NEA measure i.e. air-temperature,relative-humidity or rainfall. Default to all 3')
    parser.add_argument('--filename','-f', nargs='?', default=None,type=str, help='If provided, file to load')
    parser.add_argument('--batch','-B', action="store_true", help='If provided, trigger batch loading')
    parser.add_argument('--enddate','-e', nargs='?', default="", type=str, help='YYYY-MM-DD format. If provided, load data up to this date')
    parser.add_argument('--startdate','-s', nargs='?', default="", type=str, help='YYYY-MM-DD format. If provided, load data from this date')
    args = parser.parse_args()
    
    project = args.project
    bucket = args.bucket
    dataset_id = args.dataset_id
    measure = args.measure
    filename = args.filename
    batch=args.batch
    start_date = args.startdate
    end_date = args.enddate
    
    if measure:
        measures = [measure]
    else:
        measures = ['rainfall','air-temperature','relative-humidity']

    for measure in measures:
        fs = gcsfs.GCSFileSystem(project=project)
        
        fs.glob('/'.join([bucket,measure,"*"]))
        filenames = fs.glob('/'.join([bucket,measure,"*"]))

        if batch:              
            start_index = get_start_index(start_date,filenames)
            end_index = get_end_index(end_date,filenames)
            filenames = filenames[start_index:end_index]

            for file in filenames:
                load_nea_to_gbq(project,dataset_id,measure,file)
                    
            print("Last file loaded.")

        else:
            # If not batch, then single file processing
            if filename:
                # Use the filename provided
                filename='/'.join([bucket,measure,filename])
            else:
                # No filename provided, default to latest file      
                print("No filename provided, default to latest file")      
                filename=filenames[-1]

            load_nea_to_gbq(project,dataset_id,measure,filename)
    
    