import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import json
import importlib

# Check if pandas is installed
try:
    importlib.import_module('pandas')
except ImportError:
    import subprocess
    import sys

    print("pandas is not installed. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])

import pandas as pd

Cid = "80a1bc7bdc4a4b0492d4fedbcf52387d"
Csecret = "c6079444278846479ae2b186a38ff4d0"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=Cid,
                                               client_secret=Csecret,
                                               redirect_uri="http://localhost:3000",
                                               scope="user-library-read"))

def run_spotify_etl():
    taylor_uri = 'spotify:artist:06HL4z0CvFAxyc27GXpf02'
    results = sp.artist_albums(taylor_uri, album_type='album')
    albums = results['items']
    while results['next']:
        results = sp.next(results)
        albums.extend(results['items'])

    list = []
    for album in albums:
        refined_text = {
            "name": album['name'],
            "release_date": album['release_date'],
            "total_tracks": album['total_tracks']
        }
        list.append(refined_text)

    pd.DataFrame(list).to_csv('taylor_swift_albums.csv', index=False)