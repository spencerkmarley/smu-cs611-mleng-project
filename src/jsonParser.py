import json
import pandas as pd

class jsonParser():
    def __init__(self,fs=None):
        '''
        Args:
            fs: gcsfs.GCSFileSystem object
        Comments:
            If fs is present, uses gcsfs context to open file.
            Otherwise, a local filesystem context is used.
        '''
        self.fs=fs

    def get_items(self,path:str,kind:str):
        '''Parse items key from JSON
        Args:
            path: Path to file
            kind: Display for description i.e. 'Relative Humidity'
            cloud: Flag to toggle reading Google Cloud Storage or local file systems
            
        Returns:
            pandas DataFrame object of the following schema:
                - timestamp
                - station_id
                - value
                - Description
        '''
        if self.fs!=None:
            with self.fs.open(path) as f:
                data = json.load(f)
        else:
            with open(path,'r') as f:
                data = json.load(f)
        
        items = data['items'][0]
        
        df = pd.DataFrame({
            'timestamp':[items['timestamp'] for i in range(len(items['readings']))],
            'station_id':[[(k,v) for k,v in obj.items()][0][1] for obj in items['readings']],
            'value':[[(k,v) for k,v in obj.items()][1][1] for obj in items['readings']],
            'Description':[kind for i in range(len(items['readings']))]})
        
        df['value']=df['value'].astype('float')
        df['timestamp']=pd.to_datetime(df['timestamp'])

        return df


    def get_metadata(self,path:str,kind:str):
        '''Parse metadata key from JSON
        Args:
            path: Path to file
            kind: Display for description
            cloud: Flag to toggle reading Google Cloud Storage or local file systems

        Returns:
            pandas DataFrame object of the following schema:
                - timestamp            
                - name
                - latitude
                - longitude
                - station
                - reading_type
                - reading_unit
                - Description                
        '''

        if self.fs!=None:
            with self.fs.open(path) as f:
                data = json.load(f)
        else:
            with open(path,'r') as f:
                data = json.load(f)
        
        metadata = data['metadata']
        
        timestamp = [data['items'][0]['timestamp'] for i in range(len(metadata['stations']))]
        location = [station['location'] for station in metadata['stations']]
        latitude = [station['latitude'] for station in location]
        longitude = [station['longitude'] for station in location]
        name = [station['name'] for station in metadata['stations']]
        station = [station['id'] for station in metadata['stations']]
        reading_type = metadata['reading_type']
        reading_unit = metadata['reading_unit']
        
        df = pd.DataFrame(
            {
                'timestamp':timestamp,            
                'name':name,
                'latitude':latitude,
                'longitude':longitude,
                'station':station,
                'reading_type':[reading_type for i in range(len(metadata['stations']))],
                'reading_unit':[reading_unit for i in range(len(metadata['stations']))],
                'Description':[kind for i in range(len(metadata['stations']))]
            }
        )

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        

        return df

    def load_taxi_data(self,path):
        '''Parse taxi coordinates from JSON
        Args:
            path: Path to file
            cloud: Flag to toggle reading Google Cloud Storage or local file systems
            
        Returns:
            pandas DataFrame object of the following schema:
                - timestamp	
                - longitude
                - latitude
                
        '''
        if self.fs!=None:
            with self.fs.open(path) as f:
                lta_data=json.load(f)

        else:
            with open(path,'r') as f:
                lta_data=json.load(f)
        
        features = lta_data['features'][0]
        coordinates = features['geometry']['coordinates']
        longitude = [i[0] for i in coordinates]
        latitude = [i[1] for i in coordinates]
        timestamp = [features['properties']['timestamp'] for i in range(len(coordinates))]
        df = pd.DataFrame({'timestamp':timestamp,'longitude':longitude,'latitude':latitude})
        return df


if __name__=='__main__':
    json_parser=jsonParser()
    rainfall_items = json_parser.get_items('data/rainfall_2022-05-24T22-21-01.json','Rainfall')
    rainfall_metadata = json_parser.get_metadata('data/rainfall_2022-05-24T22-21-01.json','Rainfall')
    relative_humidity_items = json_parser.get_items('data/relative-humidity_2022-05-24T22-21-01.json','Relative Humidity')
    relative_humidity_metadata = json_parser.get_metadata('data/relative-humidity_2022-05-24T22-21-01.json','Relative Humidity')    
    air_temperature_items = json_parser.get_items('data/air-temperature_2022-05-24T22-21-01.json','Air Temperature')
    air_temperature_metadata = json_parser.get_metadata('data/air-temperature_2022-05-24T22-21-01.json','Air Temperature')
    taxi_data = json_parser.load_taxi_data('data/taxis_2022-05-27T14-00-03.json')

    
    rainfall_items.to_csv('rainfall_items.csv')
    rainfall_metadata.to_csv('rainfall_metadata.csv')
    relative_humidity_items.to_csv('relative_humidity_items.csv')
    relative_humidity_metadata.to_csv('relative_humidity_metadata.csv')
    air_temperature_items.to_csv('air_temperature_items.csv')
    air_temperature_metadata.to_csv('air_temperature_metadata.csv')
    taxi_data.to_csv('taxi_data.csv')