import time
import pandas as pd
from tqdm import tqdm
import argparse
import spotify_unwrapped as spy
from spotify_unwrapped import logger
from spotify_unwrapped.spotify_api import SpotiyAPI

spotifyapi = SpotiyAPI()
data_directory = spy.PROJECT_DIRECTORY / 'data'


def get_track_metadata(tracks):
    """Get track metadata in batches."""
    metadata = {}
    for idx in tqdm(range(0, len(tracks) - 1, 50)):
        tracks_batch = tracks[idx:min(idx + 50, len(tracks))]
        try:
            keep_attempting_request = True
            while keep_attempting_request:
                data_batch = spotifyapi.get_several_tracks(tracks_batch)
                keep_attempting_request = spotifyapi.is_rate_limited or spotifyapi.is_token_expired
                if spotifyapi.is_token_expired:
                    logger.info(f'Access token expired - refreshing')
                    spotifyapi.refresh_access_token_and_headers()
                if spotifyapi.is_rate_limited:
                    logger.info(f'Rate limited... sleeping for 30s')
                    time.sleep(30)
        except Exception as err:
            logger.info(f'Exception hit for idx {idx}: {err}')
            data_batch = err

        metadata.update(zip(tracks_batch, data_batch['tracks']))

    mdf = pd.DataFrame.from_dict(metadata, orient='index')
    mdf.index.name = COL_TRACK_ID
    mdf.to_csv(data_directory / 'track_metadata.csv')


def get_track_metadata_from_search() -> None:
    """Used for searching for (artist,track) and assuming first result is the relevent entry.
    Initially used to get track id but later realised this is included in the listening history (`spotify_track_uri`).
    """
    metadata = {}
    for artist, track_name in tqdm(artist_track_pairs):
        keep_attempting_request = True
        data = None
        try:
            while keep_attempting_request:
                data = spotifyapi.get_metadata_for_track_search(artist, track_name)
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


def get_artist_metadata_from_search(artists: list[str]) -> None:
    """Used for searching for (artist,track) and assuming first result is the relevent entry.
    Initially used to get track id but later realised this is included in the listening history (`spotify_track_uri`).
    """
    metadata = {}
    print(f"Requesting artist data on {len(artists)} artists.")
    for artist in tqdm(artists):
        keep_attempting_request = True
        data = None
        try:
            while keep_attempting_request:
                data = spotifyapi.get_metadata_for_artist_search(artist)
                keep_attempting_request = spotifyapi.is_rate_limited or spotifyapi.is_token_expired
                if spotifyapi.is_token_expired:
                    print('Access token expired - refreshing')
                    spotifyapi.refresh_access_token_and_headers()
                if spotifyapi.is_rate_limited:
                    print('Rate limited...')
                    time.sleep(30)
        except Exception as err:
            print(f'Exception hit for {artist}: {err.__repr__()}')
            data = err
        metadata[artist] = data

    mdf = pd.DataFrame.from_dict(metadata, orient='index')
    mdf.to_csv(data_directory / 'artist_metadata.csv')


schema_audio_features = {
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
    """Read in streaming history and use track_ids to request audio features."""
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
                    logger.info(f'Access token expired - refreshing')
                    spotifyapi.refresh_access_token_and_headers()
                if spotifyapi.is_rate_limited:
                    logger.info(f'Rate limited... sleeping for 30s')
                    time.sleep(30)
            for track_id, features in zip(df_subset.track_id, data['audio_features']):
                if features is None:
                    features = schema_audio_features
                audio_features[track_id] = features
        except Exception as err:
            logger.info(f'Exception hit for idx {idx}: {err}')

    # # Save out raw json incase pandas step breaks...
    # audio_features_json = {}
    # for k, v in audio_features.items():
    #     audio_features_json[k] = v
    # with open(data_directory / 'audio_features.json', 'w') as f:
    #     json.dump(audio_features_json, f)
    # with open(data_directory / 'audio_features.json', 'r') as f:
    #     audio_features_json_load = json.load(f)
    afdf = pd.DataFrame.from_dict(audio_features, orient='index')
    afdf.to_csv(data_directory / 'audio_features.csv')


if __name__ == '__main__':
    valid_tasks = ('get_audio_features', 'get_artist_metadata_from_search', 'get_track_metadata', )
    parser = argparse.ArgumentParser()
    parser.add_argument("-t",  "--task", help=f"name of task to run. must be one of {valid_tasks}",
                        type=str, required=False)
    args = parser.parse_args()
    if args.task not in valid_tasks:
        logger.error(f"Task name not valid. Must be one of {valid_tasks}")
        exit(1)

    COL_TRACK_ID = 'track_id'
    df_full = spy.load_streaming_history(data_directory=data_directory, extended=True)
    artist_track_pairs = df_full[['artistName', 'trackName']].drop_duplicates().values
    unique_artists = df_full.artistName.dropna().drop_duplicates().values
    unique_tracks = df_full[COL_TRACK_ID].dropna().drop_duplicates().values

    match args.task:
        case "get_audio_features":
            get_audio_features()
        case "get_artist_metadata_from_search":
            get_artist_metadata_from_search(unique_artists)
        case "get_track_metadata":
            get_track_metadata(unique_tracks)
