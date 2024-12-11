import json
import time
import pandas as pd
from tqdm import tqdm
from typing import List
import argparse
import spotify_unwrapped as su
from spotify_unwrapped import logger
from spotify_unwrapped.api import SpotiyAPI

api = SpotiyAPI()
data_directory = su.PROJECT_DIRECTORY / 'data'


def get_track_metadata(tracks: List[str], dump_raw_json=False) -> None:
    """Get track metadata in batches."""
    metadata = {}
    for idx in tqdm(range(0, len(tracks) - 1, 50)):
        tracks_batch = tracks[idx:min(idx + 50, len(tracks))]
        try:
            keep_attempting_request = True
            while keep_attempting_request:
                data_batch = api.get_several_tracks(tracks_batch)
                keep_attempting_request = api.is_rate_limited or api.is_token_expired
                if api.is_token_expired:
                    logger.info(f'Access token expired - refreshing')
                    api.refresh_access_token_and_headers()
                if api.is_rate_limited:
                    logger.info(f'Rate limited... sleeping for 30s')
                    time.sleep(30)
        except Exception as err:
            logger.info(f'Exception hit for idx {idx}: {err}')
            data_batch = err

        metadata.update(zip(tracks_batch, data_batch['tracks']))


    # Save out raw json incase pandas step breaks...
    if dump_raw_json:
        metadata_json = {}
        for k, v in metadata.items():
            metadata_json[k] = v
        with open(data_directory / 'track_metadata.json', 'w') as f:
            json.dump(metadata_json, f)
    mdf = pd.DataFrame.from_dict(metadata, orient='index')
    mdf.index.name = COL_TRACK_ID
    mdf.to_csv(data_directory / 'track_metadata.csv')


def get_track_metadata_from_search(dump_raw_json=False) -> None:
    """Used for searching for (artist,track) and assuming first result is the relevent entry.
    Initially used to get track id but later realised this is included in the listening 
    history (`spotify_track_uri`).
    """
    metadata = {}
    for artist, track_name in tqdm(artist_track_pairs):
        keep_attempting_request = True
        data = None
        try:
            while keep_attempting_request:
                data = api.get_metadata_for_track_search(artist, track_name)
                keep_attempting_request = api.is_rate_limited or api.is_token_expired
                if api.is_token_expired:
                    logger.info('Access token expired - refreshing')
                    api.refresh_access_token_and_headers()
                if api.is_rate_limited:
                    logger.info('Rate limited... sleeping for 30s')
                    time.sleep(30)
        except Exception as err:
            logger.warning(f'Exception hit for {artist} {track_name}: {err}')
            data = err
        metadata[(artist, track_name)] = data

    # Save out raw json incase pandas step breaks...
    if dump_raw_json:
        metadata_json = {}
        for k, v in metadata.items():
            metadata_json[k] = v
        with open(data_directory / 'track_metadata_from_search.json', 'w') as f:
            json.dump(metadata_json, f)
    mdf = pd.DataFrame.from_dict(metadata, orient='index')
    mdf.to_csv(data_directory / 'track_metadata.csv')


def get_artist_metadata_from_search(artists: list[str], dump_raw_json=False) -> None:
    """Used for searching for (artist,track) and assuming first result is the relevent entry.
    Initially used to get track id but later realised this is included in the listening 
    history (`spotify_track_uri`).
    """
    metadata = {}
    logger.info(f"Requesting artist data on {len(artists)} artists.")
    for artist in tqdm(artists):
        keep_attempting_request = True
        data = None
        try:
            while keep_attempting_request:
                data = api.get_metadata_for_artist_search(artist)
                keep_attempting_request = api.is_rate_limited or api.is_token_expired
                if api.is_token_expired:
                    logger.info('Access token expired - refreshing')
                    api.refresh_access_token_and_headers()
                if api.is_rate_limited:
                    logger.info('Rate limited...')
                    time.sleep(30)
        except Exception as err:
            logger.info(f'Exception hit for {artist}: {err.__repr__()}')
            data = str(err)
        metadata[artist] = data

    # Save out raw json incase pandas step breaks...
    if dump_raw_json:
        metadata_json = {}
        for k, v in metadata.items():
            metadata_json[k] = v
        with open(data_directory / 'artist_metadata_from_search.json', 'w') as f:
            json.dump(metadata_json, f)

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


if __name__ == '__main__':
    valid_tasks = ('artist_metadata_from_search', 'artist_metadata_from_search', 'track_metadata', )
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",  "--task", help=f"name of task to run. must be one of {valid_tasks}",
        choices=valid_tasks, type=str, required=True)
    # converting raw data to csv sometimes fails after a long set of requests - this can be used to
    # dump the raw json to disk for manual intervention
    parser.add_argument(
        "-d",  "--dump_raw_json", help=f"dump raw json for artist metadata", action='store_true')
    args = parser.parse_args()

    COL_TRACK_ID = 'track_id'
    df_full = su.load_streaming_history("MyData", extended=True)
    artist_track_pairs = df_full[['artistName', 'trackName']].drop_duplicates().values

    unique_artists = df_full.artistName.dropna().drop_duplicates().values
    unique_tracks = df_full[COL_TRACK_ID].dropna().drop_duplicates().values

    match args.task:
        case "track_metadata":
            get_track_metadata(unique_tracks)
        case "track_metadata_from_search":
            get_track_metadata_from_search(unique_tracks, dump_raw_json=args.dump_raw_json)
        case "artist_metadata_from_search":
            get_artist_metadata_from_search(unique_artists, dump_raw_json=args.dump_raw_json)
        case "audio_features":
            raise Exception("No longer available")
