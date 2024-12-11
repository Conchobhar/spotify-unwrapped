import zipfile
import re
from ast import literal_eval
import logging

import pandas as pd
from pathlib import Path

PROJECT_DIRECTORY = Path(__file__).parents[1]

DATA_DIRECTORY = PROJECT_DIRECTORY / 'data'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s: %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("spotify_unwrapped")


def load_streaming_history(source:str=None, extended:bool=False) -> pd.DataFrame:
    """Load in streaming records from zipped data or a directory.

    If no source_path provided, assumes we're reading the my_spotify_data.zip file as supplied by spotify.

    Set source_path to a directory under DATA_DIRECTORY if that directory contains a collection
    of history files from various time points.

    :param extended: Is this the extended streaming history?
    :return: DataFrame of combined streaming history
    """
    df = None
    if source is None:
        for file in DATA_DIRECTORY.glob('*.zip'):
            if 'my_spotify_data' in file.name:
                source_path = DATA_DIRECTORY / file
                break
        if source is None:
            logger.error(f"Could not find a zip file in {DATA_DIRECTORY}")
            return
    else:
        if not (DATA_DIRECTORY/source).exists():
            logger.error(f"File {source} does not exist.")
            return
        source_path = DATA_DIRECTORY / source
    filename_hx = f"Streaming"
    pattern_hx = re.compile("20[0-9]{2}_?[0-9]{1,2}.json")
    dfs = []
    # Rename columns to match spotify data obtained from the last year request
    col_full_to_past_year = {
        'ts': 'endTime',
        'master_metadata_track_name': 'trackName',
        'master_metadata_album_artist_name': 'artistName',
        'master_metadata_album_album_name': 'albumName',
    }
    if source_path.suffix == '.zip':
        with zipfile.ZipFile(source) as z:
            for filename in z.namelist():
                filepath = Path(filename)
                if filepath.name.startswith(filename_hx) or pattern_hx.match(filepath.name):
                    with z.open(filename) as f:
                        dfs.append(pd.read_json(f))
    elif source_path.is_dir():
        for file in source_path.glob('*.json'):
            if file.name.startswith(filename_hx) or pattern_hx.match(file.name):
                dfs.append(pd.read_json(file).rename(columns=col_full_to_past_year))
                        
    df = pd.concat([*dfs])
    df = df.dropna(subset=['artistName', 'trackName']).sort_values(by='endTime')
    logger.info(f"Dropping {df.endTime.duplicated().sum()} duplicates by 'endTime' field...")
    df = df.drop_duplicates(subset='endTime')
    df['track_id'] = df.spotify_track_uri.apply(lambda x: x.split(':')[-1] if isinstance(x, str) else None)
    return df


class SpotifyData:

    def __init__(self):
        df_full_cols = ['track_id', 'endTime', 'ms_played', 'conn_country', 'trackName', 'artistName', 'albumName',
                        'spotify_track_uri', 'reason_start', 'reason_end', 'shuffle', 'skipped', ]
        self.df_full = load_streaming_history()[df_full_cols]

        mdf_cols = ['id', 'popularity']
        self.mdf = pd.read_csv(DATA_DIRECTORY / 'track_metadata.csv', index_col=[0, 1])[mdf_cols]

        afdf_cols = ['id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
                     'instrumentalness', 'liveness', 'valence', 'tempo', ]
        self.afdf = pd.read_csv(DATA_DIRECTORY / 'audio_features.csv', index_col=[0])[afdf_cols]

        artist_df_cols = ['id', 'genres', 'popularity', 'followers', ]
        self.artist_df = pd.read_csv(DATA_DIRECTORY / 'artist_metadata.csv', index_col=[0])[artist_df_cols].dropna()

        self.artist_df.genres = self.artist_df.genres.apply(literal_eval)
        self.df_full.endTime = self.df_full.endTime.apply(lambda x: pd.to_datetime(x[0:-1]))

        self.df = (
            self.df_full
            .merge(self.mdf, how='left', left_on=['artistName', 'trackName'], right_index=True)
            .merge(self.afdf, how='left', left_on='track_id', right_on='id')
            .merge(self.artist_df['genres'], how='left', left_on='artistName', right_index=True)
            .set_index('endTime').sort_index())
