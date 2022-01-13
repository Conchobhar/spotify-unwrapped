import os
import json
import zipfile
import pandas as pd
from pathlib import Path

PROJECT_DIRECTORY = Path(__file__).parents[1]

DATA_DIRECTORY = PROJECT_DIRECTORY / 'data'


def load_streaming_history(data_directory=DATA_DIRECTORY, full=True):
    data_directory = Path(data_directory)
    df = None
    filename_zip = data_directory / f"my_spotify_data{'_full' if full else ''}.zip"
    filename_hx = f"MyData/{'endsong' if full else 'StreamingHistory'}"
    with zipfile.ZipFile(filename_zip) as z:
        for filename in z.namelist():
            if filename.startswith(filename_hx):
                with z.open(filename) as f:
                    if df is None:
                        df = pd.read_json(f)
                    else:
                        df = df.append(pd.read_json(f))

    # Rename columns to match spotify data obtained from the last year request
    col_full_to_past_year = {
        'ts': 'endTime',
        'master_metadata_track_name': 'trackName',
        'master_metadata_album_artist_name': 'artistName',
        'master_metadata_album_album_name': 'albumName',
    }
    df = df.rename(columns=col_full_to_past_year).dropna(subset=['artistName', 'trackName']).sort_values(by='endTime')
    print(f"Dropping {df.endTime.duplicated().sum()} duplicates...")
    df = df.drop_duplicates(subset='endTime')
    df['track_id'] = df.spotify_track_uri.apply(lambda x: x.split(':')[-1])
    return df
