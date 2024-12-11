import json
import requests
from enum import Enum
from requests import JSONDecodeError, Response
from urllib.parse import quote

from spotify_unwrapped import PROJECT_DIRECTORY, logger

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
get_several_tracks_endpoint = f"{SPOTIFY_BASE_URL}tracks/"


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
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }
        auth_response = requests.post(
            SPOTIFY_AUTH_URL,
            data=data
        )
        auth_response_data = auth_response.json()
        if auth_response.status_code == 200:
            self.access_token = auth_response_data['access_token']
            self.headers = {'Authorization': f'Bearer {self.access_token}'}

        else:
            raise Exception(f"Auth status code returned {auth_response.status_code}: {auth_response_data=}. Is your"
                            f"credentials file correct?")

    def make_get_request(self, endpoint: str, headers: dict = None) -> Response:
        if headers is None:
            headers = self.headers
        response = requests.get(endpoint, headers=headers)
        self.last_status_code = response.status_code
        if self.last_status_code == ErrorCodes.SUCCESS.value:
            return response
        elif self.last_status_code == ErrorCodes.RATE_LIMITED.value:
            logger.warning(f"get request returned error code {self.last_status_code} (rate limited)")
        elif self.last_status_code == ErrorCodes.AUTH_EXPIRED.value:
            logger.warning(f"get request returned error code {self.last_status_code} (auth expired)")
        else:
            logger.warning(f"get request returned error code {self.last_status_code}")
        return response

    def get_batch_request(self, request_name, ids, max_batch_size=None):
        """Batch request
        Batch must not exceed max_batch_size.
        """
        if len(ids) > max_batch_size:
            raise Exception(f"Batch size {len(ids)} exceeds max_batch_size {max_batch_size}")
        endpoint = f"{request_name}?ids={','.join(ids)}"
        search_request = self.make_get_request(endpoint, headers=self.headers)
        data = search_request.json()
        return data

    def get_tracks_audio_features(self, ids: list):
        """Batch request for audio features.
        """
        max_batch_size_get_tracks_audio_features = 100
        return self.get_batch_request(get_tracks_audio_features_endpoint, ids, max_batch_size_get_tracks_audio_features)

    def get_several_tracks(self, ids: list):
        """Batch request for track metadata.
        """
        max_batch_size_get_several_track_audio_features = 50
        return self.get_batch_request(get_several_tracks_endpoint, ids, max_batch_size_get_several_track_audio_features)

    def get_artist_metadata_from_track_id(self):
        """From listening history, get unqiue artists and any track id of theirs.
        use track id to get artist id
        use artist id to get metadata (batch)
        """
        raise NotImplementedError()

    def get_metadata_for_track_search(self, artist: str, track: str) -> dict:
        """Get meta data for a specific artist/track which we do not have the track_id for, assuming the first result
        will be the relevent one.
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

    def get_metadata_for_artist_search(self, artist: str) -> dict:
        """Get meta data for a specific artist/track which we do not have the id for, assuming the first result will
        be the relevent one.
        We run this for each unique (artist, track) in our history to get metadata for them.

        :param artist: string for artist
        :return: Dictionary of relevant meta data
        """
        # construct query
        endpoint = f"{SPOTIFY_BASE_URL}search?q=artist:'{quote(artist)}'&type=artist"
        search_request = self.make_get_request(endpoint, headers=self.headers)
        # validate search_request can use json method before tring
        try:
            data = search_request.json()
        except JSONDecodeError:
            data = str(search_request.content)
        metadata = {'status_code': search_request.status_code, 'nitems': None, 'href': None, 'id': None,
                    'popularity': None}

        if search_request.status_code == 200:
            items = data['artists']['items']
            metadata['nitems'] = len(items)
            if len(items) > 0:
                item = items[0]  # Assume first item is what we want
                metadata['href'] = item['href']
                metadata['id'] = item['id']
                metadata['name'] = item['name']
                metadata['popularity'] = item['popularity']
                metadata['followers'] = item['followers']['total']
                metadata['genres'] = item['genres']
        return metadata
