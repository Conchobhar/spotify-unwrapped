import zipfile
import re
from ast import literal_eval
import logging

import pandas as pd
from pathlib import Path

PROJECT_DIRECTORY = Path(__file__).parents[1]

DATA_DIRECTORY = PROJECT_DIRECTORY / 'data'

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spotify_unwrapped")


def load_streaming_history(filename_data=None, data_directory=DATA_DIRECTORY, extended=True):
    """Load in streaming records from zipped data file.

    :param filename_data: filename of zipped streaming data
    :param data_directory: directory `filename_data` is located in
    :param extended: Is this the extended streaming history?
    :return: DataFrame of combined streaming history
    """
    data_directory = Path(data_directory)
    df = None
    if filename_data:
        filename_zip = data_directory / filename_data
    else:
        filename_zip = data_directory / f"my_spotify_data{'_extended' if extended else ''}.zip"
        logger.info(f"Trying to load {filename_zip}")
    filename_hx = f"MyData/{'Streaming_History_Audio' if extended else 'StreamingHistory'}"
    pattern_hx = re.compile("MyData/20[0-9]{2}_?[0-9]{1,2}.json")
    with zipfile.ZipFile(filename_zip) as z:
        for filename in z.namelist():
            if filename.startswith(filename_hx) or pattern_hx.match(filename):
                with z.open(filename) as f:
                    if df is None:
                        df = pd.read_json(f)
                    else:
                        df = pd.concat([df, pd.read_json(f)])

    # Rename columns to match spotify data obtained from the last year request
    col_full_to_past_year = {
        'ts': 'endTime',
        'master_metadata_track_name': 'trackName',
        'master_metadata_album_artist_name': 'artistName',
        'master_metadata_album_album_name': 'albumName',
    }
    df = df.rename(columns=col_full_to_past_year).dropna(subset=['artistName', 'trackName']).sort_values(by='endTime')
    logger.info(f"Dropping {df.endTime.duplicated().sum()} endTime duplicates...")
    df = df.drop_duplicates(subset='endTime')
    df['track_id'] = df.spotify_track_uri.apply(lambda x: x.split(':')[-1])
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
