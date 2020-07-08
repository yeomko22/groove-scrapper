import hashlib
import os
import shutil

import requests

import util
from dbhandler import DBHandler
from gcphandler import GCPHandler


class Uploader:
    def __init__(self):
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.dbhandler = DBHandler(self.config)
        self.gcphandler = GCPHandler(self.config)
        target_ids = util.load_target_ids('target.txt')
        self.user_sids = self.dbhandler.select_user_sids(target_ids)

    def upload_per_user(self):
        tmpdir = os.path.abspath('./tmp')
        for user_sid in self.user_sids:
            track_ids = [x for x in os.listdir(os.path.join(tmpdir, str(user_sid))) if '.DS_Store' not in x]
            for track_id in track_ids:
                self.upload_hls(user_sid, track_id)
            shutil.rmtree(f'./tmp/{user_sid}')

    def upload_hls(self, user_sid, track_id):
        upload_url = self.config['UPLOADURL']
        hashstr = str(user_sid) + track_id
        hashkey = hashlib.sha1()
        hashkey.update(hashstr.encode('utf-8'))
        hlshash = hashkey.hexdigest()
        filedir = f'./tmp/{user_sid}/{track_id}'
        files = os.listdir(filedir)
        for file in files:
            if '.mp3' in file:
                continue
            response = requests.post(
                url=upload_url,
                files={'file': open(os.path.join(filedir, file), 'rb')},
                data={'dir': f'/hls/{hlshash}'}
            )
            print(response.status_code, response.text)
        self.dbhandler.update_track_hls(track_id, hlshash)


if __name__ == '__main__':
    uploader = Uploader()
    uploader.upload_per_user()
