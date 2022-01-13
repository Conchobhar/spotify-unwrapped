import requests
from urllib.parse import quote
from enum import Enum
from pathlib import Path
import json

from spotilytics import PROJECT_DIRECTORY

CREDENTIALS_FILE = PROJECT_DIRECTORY / 'credentials' / 'client_credentials.json'

if CREDENTIALS_FILE.is_file():
    creds = json.load(CREDENTIALS_FILE.open('r'))
    if 'CLIENT_ID' in creds and 'CLIENT_SECRET' in creds:
        CLIENT_ID = creds['CLIENT_ID']
        CLIENT_SECRET = creds['CLIENT_SECRET']
    else:
        CLIENT_ID = None
        CLIENT_SECRET = None

SPOTIFY_AUTH_URL = 'https://accounts.spotify.com/api/token'
SPOTIFY_BASE_URL = 'https://api.spotify.com/v1/'

get_tracks_audio_features_endpoint = f"{SPOTIFY_BASE_URL}audio-features/"


class ErrorCodes(Enum):
    SUCCESS = 200
    AUTH_EXPIRED = 401
    BAD_OAUTH = 403
    RATE_LIMITED = 429


class SpotiyAPI:

    def __init__(self, client_id=CLIENT_ID, client_secret=CLIENT_SECRET):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.headers = None
        self.refresh_access_token_and_headers()
        self.last_status_code = None

    @property
    def is_rate_limited(self):
        return self.last_status_code == ErrorCodes.RATE_LIMITED.value

    @property
    def is_token_expired(self):
        return self.last_status_code == ErrorCodes.AUTH_EXPIRED.value

    def refresh_access_token_and_headers(self):
        auth_response = requests.post(
            SPOTIFY_AUTH_URL,
            {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
            }
        )
        if auth_response.status_code == 200:
            auth_response_data = auth_response.json()
            self.access_token = auth_response_data['access_token']
            self.headers = {'Authorization': f'Bearer {self.access_token}'}

        else:
            raise Exception(f"Auth status code returned {auth_response.status_code}")

    def make_get_request(self, endpoint, headers=None):
        if headers is None:
            headers = self.headers
        search_request = requests.get(endpoint, headers=headers)
        self.last_status_code = search_request.status_code
        if self.last_status_code == ErrorCodes.SUCCESS.value:
            return search_request
        elif self.last_status_code == ErrorCodes.RATE_LIMITED.value:
            print(f"Warning: get request returned error code {self.last_status_code} (rate limited)")
        elif self.last_status_code == ErrorCodes.AUTH_EXPIRED.value:
            print(f"Warning: get request returned error code {self.last_status_code} (auth expired)")
        return search_request

    def get_several_track_audio_features(self, ids):
        assert len(ids) <= 100
        endpoint = f"{get_tracks_audio_features_endpoint}?ids={','.join(ids)}"
        search_request = self.make_get_request(endpoint, headers=self.headers)
        data = search_request.json()
        return data

    def get_spotify_metadata_for_track_search(self, artist, track):
        """Get meta data for a specific artist/track which we do not have the , assuming the first result will be the relevent one.
        We run this for each unique (artist, track) in our history to get metadata for them.

        :param artist: string for artist
        :param track: string for track
        :return: Dictionary of relevant meta data
        """
        # construct query
        endpoint = f"{SPOTIFY_BASE_URL}search?q=artist:'{quote(artist)}'%20track:'{quote(track)}'&type=track"
        search_request = self.make_get_request(endpoint, headers=self.headers)
        data = search_request.json()
        metadata = {'status_code': search_request.status_code, 'nitems': None, 'href': None, 'id': None,
                    'popularity': None}

        if search_request.status_code == 200:
            items = data['tracks']['items']
            metadata['nitems'] = len(items)
            if len(items) > 0:
                item = items[0]  # Assume first item is what we want
                metadata['href'] = item['href']
                metadata['id'] = item['id']
                metadata['popularity'] = item['popularity']

        return metadata
