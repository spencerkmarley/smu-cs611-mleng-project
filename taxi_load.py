import gcsfs
import re

from src import jsonParser

def load_taxi_to_gbq(project:str,dataset_id:str,filename:str):
    '''Load a single json 
    Args:
        project:str:        
        dataset_id:str:
        measure:str:
        filename:str:
    '''
    print(f"Current processing {filename}")
    parser = jsonParser.jsonParser(fs)

    taxi_data = parser.load_taxi_data(filename)

    print(f"Writing taxi data")
    taxi_data.to_gbq(dataset_id+'.'+'taxi-availability',project,chunksize=None,if_exists='append')

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

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Reads LTA JSON file to GBQ')
    parser.add_argument('--project','-p', nargs='?', default='ml-eng-cs611-group-project', type=str, help='GCP project name i.e. ml-eng-cs611-group-project')
    parser.add_argument('--bucket','-b', nargs='?', default='ml-eng-cs611-group-project-taxis', type=str, help='GCS bucket name i.e. ml-eng-cs611-group-project-taxis')
    parser.add_argument('--dataset_id','-d', nargs='?', default='taxi_dataset', type=str, help='GBQ dataset ID i.e. taxi_dataset')    
    parser.add_argument('--filename','-f', nargs='?', default=None,type=str, help='If provided, file to load i.e. 2022-05-27T14-00-03.json')
    parser.add_argument('--date','-D', nargs='?', default=None, type=str, help='YYYY-MM-DD format. If provided, load data up to this date')
    args = parser.parse_args()
    
    project = args.project
    bucket = args.bucket
    dataset_id = args.dataset_id    
    filename = args.filename
    date = args.date
    
    fs = gcsfs.GCSFileSystem(project=project)
        
    filenames = fs.glob('/'.join([bucket,'taxis',"*"]))

    if date:
        if date_check(date):
        # If date is provided, iteratively add files until date
            
            print(f" Valid date is provided. Backfill until {date}")
            for file in filenames:
                if date in file:
                    print("Stopping batch upload.")
                    break
                else:
                    load_taxi_to_gbq(project,dataset_id,file)
                
                print("Last file loaded.")
        else:
            print('Invalid date provided. Please enter date in format YYYY-MM-DD')
    else:
        # If no date, then single file processing
        if filename:
            # Use the filename provided
            filename='/'.join([bucket,'taxis',filename])
        else:
            # No filename provided, default to latest file            
            print("No filename provided, default to latest file")
            filename=filenames[-1]

        load_taxi_to_gbq(project,dataset_id,filename)
    
    