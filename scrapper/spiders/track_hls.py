"""
1. target 텍스트 파일 안의 아티스트들의 sid를 디비에서 읽어
2. 각 아티스트별로 track 정보를 수집
3. 각 트랙별 아트웤 이미지 파일 수집
4. 각 트랙별 m3u8 파일 수집
"""
import io
import json
import os
import warnings

import scrapy

from scrapper import util
from scrapper.dbhandler import DBHandler
from scrapper.gcphandler import GCPHandler


class TrackNoHlsSpider(scrapy.Spider):
    name = 'track_nohls'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        warnings.simplefilter("ignore")
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.target_ids = util.load_target_ids('target.txt')
        self.dbhandler = DBHandler(self.config)
        self.gcphandler = GCPHandler(self.config)
        self.track_nohls_ids = self.dbhandler.select_track_nohls()

    def parse(self, response):
        user_sids = self.dbhandler.select_all_user_sids()
        url_head = "https://api-v2.soundcloud.com/users/{0}"
        url_tail = f"/tracks?representation=&client_id={self.config['CLIENT_ID']}&limit=20&offset=0&linked_partitioning=1&app_version=1593604665&app_locale=en"
        for user_sid in user_sids:
            url = url_head.format(user_sid) + url_tail
            req = scrapy.Request(url, self.parse_tracks)
            req.meta['user_sid'] = user_sid
            yield req

    def parse_tracks(self, response):
        user_sid = response.meta['user_sid']
        result_json = json.loads(response.body)
        collections = result_json['collection']
        if not collections:
            return
        for collection in collections:
            m3u8_url = ''
            try:
                m3u8_url = collection['media']['transcodings'][0]['url']
            except:
                pass
            track_json = {
                'created_at': collection['created_at'],
                'track_id': collection['id'],
                'track_user_sid': user_sid,
                'track_title': collection['title'] if collection['title'] else '',
                'track_description': collection['description'] if collection['description'] else '',
                'track_duration': collection['duration'],
                'track_genre': collection['genre'] if collection['genre'] else '',
                'track_permalink': collection['permalink_url'] if collection['permalink_url'] else '',
                'track_likes_count': collection['likes_count'] if collection['likes_count'] else 0,
                'track_playback_count': collection['playback_count'] if collection['playback_count'] else 0,
                'track_user_id': collection['user']['permalink'] if collection['user']['permalink'] else '',
                'track_user_name': collection['user']['username'] if collection['user']['username'] else '',
            }
            track_hls = self.dbhandler.select_hls_per_track(track_json['track_id'])
            if not track_hls:
                continue
            if not track_hls[0][0] and m3u8_url:
                m3u8_req = scrapy.Request(m3u8_url + f'?client_id={self.config["CLIENT_ID"]}', self.parse_m3u8_proxy)
                m3u8_req.meta['track_json'] = track_json
                yield m3u8_req
        if result_json['next_href']:
            url = result_json['next_href'] + f'&client_id={self.config["CLIENT_ID"]}'
            req = scrapy.Request(url, self.parse_tracks)
            req.meta['user_sid'] = user_sid
            yield req

    def parse_m3u8_proxy(self, response):
        track_json = response.meta['track_json']
        m3u8_url = json.loads(response.body)['url']
        m3u8_req = scrapy.Request(m3u8_url, self.parse_m3u8)
        m3u8_req.meta['track_json'] = track_json
        yield m3u8_req

    def parse_m3u8(self, response):
        track_json = response.meta['track_json']
        track_id = track_json['track_id']
        track_user_sid = track_json['track_user_sid']
        artistdir = f'./tmp/{track_user_sid}'
        if not os.path.exists(artistdir):
            os.mkdir(artistdir)
        trackdir = f'{artistdir}/{track_id}'
        if not os.path.exists(trackdir):
            os.mkdir(trackdir)
        m3u8_path = f'{trackdir}/{track_id}.m3u8'
        mp3_path = f'{trackdir}/{track_id}.mp3'
        playlist_path = f'{trackdir}/playlist.m3u8'
        ts_format = f'{trackdir}/output%03d.ts'

        with open(m3u8_path, 'wb') as output:
            output.write(io.BytesIO(response.body).read())
        os.system(f'ffmpeg -protocol_whitelist file,http,https,tcp,tls,crypto -i {m3u8_path} -c copy {mp3_path}')
        os.system(
            f'ffmpeg -i {mp3_path} -c:a libmp3lame -b:a 128k -f segment -segment_time 30 -segment_list {playlist_path} -segment_format mpegts {ts_format}')
        os.remove(m3u8_path)
