import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime


USERID = "31wnznpcklt2fzywdf36at3q5id4"
API_TOKEN = " "

#Extract Data
def extractData() -> pd.DataFrame:
    #Spotify Headers
    spotify_headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {API_TOKEN}"
    }

    #Spotify API response
    spotify_response = requests.get("https://api.spotify.com/v1/browse/new-releases?country=US", headers = spotify_headers)

    data = spotify_response.json()

    #Store Song Details in Lists
    artist_names = []
    album_type = []
    album_name=[]
    total_tracks = []
    released_date=[]
    id=[]

    try :
        for index in range(len(data['albums']['items'])):
            artist=[]
            for total_artist in range(len(data['albums']['items'][index]['artists'])):
                artist.append((data['albums']['items'][index]['artists'][total_artist]['name']))
            artist_names.append(",".join(name for name in artist))
            
            album_type.append(data['albums']['items'][index]['album_type'])
            album_name.append(data['albums']['items'][index]['name'])
            total_tracks.append(data['albums']['items'][index]['total_tracks'])
            released_date.append(data['albums']['items'][index]['release_date'])
            id.append(str(data['albums']['items'][index]['id']))

    except KeyError as e:
        print('API Token has expired')
    
    #Load Song Lists in Dictionary for Pandas DataFrame  

    recent_released_albums = {
        'artist_name' : artist_names,
        'album_type' : album_type,
        'album_name' : album_name,
        'total_tracks' : total_tracks,
        'released_date' : released_date,
        'id' : id
    }

    #Load Dict Data in DataFrame
    album_df = pd.DataFrame(recent_released_albums,columns=['artist_name','album_type','album_name','total_tracks','released_date','id'])
    return album_df

#Data Validation
def transformData(df : pd.DataFrame) -> bool:
    #Check if DataFrame is empty
    if df.empty:
        print("No Albums Downoaded. Finished Execution")
        return False
        
    #Primary key Check -> All album released by artist
    if pd.Series(df['id']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    #Check for Null
    if df.isnull().values.any():
        raise Exception("Null Values Found")
    return True

def loadData(df : pd.DataFrame):

    database="postgres"
    user="postgres"
    password=" "
    host=" "
    port="5432"

    conn = sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}", echo=False)
    sql_query = """
        CREATE TABLE IF NOT EXISTS new_album_released(
            artist_name VARCHAR(200),
            album_type VARCHAR(100),
            album_name VARCHAR(200),
            total_tracks INT,
            released_date VARCHAR(200),
            id VARCHAR(200),
            PRIMARY KEY (id) 
        )
    """
    conn.execute(sql_query)

    print('Table Created')

    try :
        df.to_sql(name='new_album_released', con=conn, if_exists = 'append', index=False)
    except :
        print('Album already exists in database')

if __name__ == '__main__':
    song_df = extractData()

    transformData(song_df)

    loadData(song_df)
    


