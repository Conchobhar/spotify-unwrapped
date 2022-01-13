# load in spotify data
import time
import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from IPython import embed
import spotilytics as spy
from spotilytics.spotify_api import SpotiyAPI

spotifyapi = SpotiyAPI()
data_directory = spy.PROJECT_DIRECTORY / 'data'

df_full = spy.load_streaming_history(data_directory=data_directory, full=True)
artist_track_pairs = df_full[['artistName', 'trackName']].drop_duplicates().values


def get_metadata():
    """Used for searching for (artist,track) and assuming first result is the relevent entry.
    Initially used to get track id but later realised this is included in the listening history (`spotify_track_uri`).
    """
    metadata = {}
    for artist, track_name in tqdm(artist_track_pairs[1000::]):
        keep_attempting_request = True
        data = None
        try:
            while keep_attempting_request:
                data = spotifyapi.get_spotify_metadata_for_track_search(artist, track_name)
                keep_attempting_request = spotifyapi.is_rate_limited or spotifyapi.is_token_expired
                if spotifyapi.is_token_expired:
                    print('Access token expired - refreshing')
                    spotifyapi.refresh_access_token_and_headers()
                if spotifyapi.is_rate_limited:
                    print('Rate limited...')
                    time.sleep(30)
        except Exception as err:
            print(f'Exception hit for {artist} {track_name}: {err}')
            data = err
        metadata[(artist, track_name)] = data

    mdf = pd.DataFrame.from_dict(metadata, orient='index')
    mdf.to_csv(data_directory / 'track_metadata.csv')


audio_features_empty = {
    'danceability': None,
    'energy': None,
    'key': None,
    'loudness': None,
    'mode': None,
    'speechiness': None,
    'acousticness': None,
    'instrumentalness': None,
    'liveness': None,
    'valence': None,
    'tempo': None,
    'type': None,
    'id': None,
    'uri': None,
    'track_href': None,
    'analysis_url': None,
    'duration_ms': None,
    'time_signature': None,
}


def get_audio_features():
    df = df_full.dropna(subset=['track_id'])
    audio_features = {}
    for idx in tqdm(range(0, df.shape[0] - 1, 100)):
        df_subset = df.iloc[idx:min(idx + 100, df.shape[0])]
        data = {'audio_features': [None, ]}
        try:
            keep_attempting_request = True
            while keep_attempting_request:
                data = spotifyapi.get_several_track_audio_features(df_subset.track_id.values)
                keep_attempting_request = spotifyapi.is_rate_limited or spotifyapi.is_token_expired
                if spotifyapi.is_token_expired:
                    print('Access token expired - refreshing')
                    spotifyapi.refresh_access_token_and_headers()
                if spotifyapi.is_rate_limited:
                    print('Rate limited...')
                    time.sleep(30)
            for artist, features in zip(df_subset.track_id, data['audio_features']):
                if features is None:
                    features = audio_features_empty
                audio_features[artist] = features
        except Exception as err:
            print(f'Exception hit for idx {idx}: {err}')

    # Save out raw json incase pandas step breaks...
    audio_features_json = {}
    for k, v in audio_features.items():
        audio_features_json['_ARTIST_TRACK_'.join(k)] = v  # Need to join tuple key to save
    with open(data_directory / 'audio_features.json', 'w') as f:
        json.dump(audio_features_json, f)
    # with open(data_directory / 'audio_features.json', 'r') as f:
    #     audio_features_json_load = json.load(f)
    afdf = pd.DataFrame.from_dict(audio_features, orient='index')
    afdf.to_csv(data_directory / 'audio_features.csv')


if __name__ == '__main__':
    get_audio_features()
