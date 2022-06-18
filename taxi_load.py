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

def get_start_index(start_file:str,file_list:list):
    '''Iteratively search from start of file list to find first occurence of end_file.
    Args:
        start_file (str): Search string. Can give a date of form YYYY-MM-DD to find first file at specific date, or filename to match specific file.
        file_list (list): List of file paths from Google Cloud Storage.
    Yields:
        start_index (int): Index of last occurrence of search string.
    '''
    start_index=0

    
    file_re = re.compile(start_file)
    
    for file in file_list:
        if file_re.findall(file):
            start_index=file_list.index(file)
            print(f"Valid start file provided, starting batch loading at index {start_index}")
            break

    return start_index

def get_end_index(end_file:str,file_list:list):
    '''Iteratively search from end of file list to find last occurence of end_file.
    Args:
        end_file (str): Search string. Can give a date of form YYYY-MM-DD to find last file at specific date, or filename to match specific file.
        file_list (list): List of file paths from Google Cloud Storage.
    Yields:
        end_index (int): Index of last occurrence of search string.
    '''
    end_index=len(file_list)
    
    file_re = re.compile(end_file)
    for file in file_list[::-1]:
        if file_re.findall(file):
            end_index=file_list.index(file)
            print(f"Valid end date provided, end batch loading at index {end_index}")
            break
    
    return end_index

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Reads LTA JSON file to GBQ')
    parser.add_argument('--project','-p', nargs='?', default='ml-eng-cs611-group-project', type=str, help='GCP project name i.e. ml-eng-cs611-group-project')
    parser.add_argument('--bucket','-b', nargs='?', default='ml-eng-cs611-group-project-taxis', type=str, help='GCS bucket name i.e. ml-eng-cs611-group-project-taxis')
    parser.add_argument('--dataset_id','-d', nargs='?', default='taxi_dataset', type=str, help='GBQ dataset ID i.e. taxi_dataset')    
    parser.add_argument('--filename','-f', nargs='?', default=None,type=str, help='If provided, file to load i.e. 2022-05-27T14-00-03.json')
    parser.add_argument('--batch','-B', action="store_true", help='If provided, trigger batch loading')
    parser.add_argument('--endfile','-e', nargs='?', default="", type=str, help='If provided, load data up to this file/date')
    parser.add_argument('--startfile','-s', nargs='?', default="", type=str, help='If provided, load data from this file/date')
    args = parser.parse_args()
    
    project = args.project
    bucket = args.bucket
    dataset_id = args.dataset_id    
    filename = args.filename
    batch=args.batch
    start_file = args.startfile
    end_file = args.endfile
    
    fs = gcsfs.GCSFileSystem(project=project)
        
    filenames = fs.glob('/'.join([bucket,'taxis',"*"]))

    if batch:
        start_index = get_start_index(start_file,filenames)
        end_index = get_end_index(end_file,filenames)
        filenames = filenames[start_index:end_index]
        for file in filenames:
            load_taxi_to_gbq(project,dataset_id,file)
        print("Last file loaded.")
        
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
    
    