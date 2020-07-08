"""
1. target 텍스트 파일 안의 아티스트들의 sid를 디비에서 읽어
2. 각 아티스트별로 track 정보를 수집
3. 각 트랙별 아트웤 이미지 파일 수집
4. 각 트랙별 m3u8 파일 수집
"""
import json
import warnings

import scrapy

from scrapper import util
from scrapper.dbhandler import DBHandler
from scrapper.gcphandler import GCPHandler


class TrackPopularitySpider(scrapy.Spider):
    name = 'track_popularity'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        warnings.simplefilter("ignore")
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.target_ids = util.load_target_ids('target.txt')
        self.dbhandler = DBHandler(self.config)
        self.gcphandler = GCPHandler(self.config)

    def parse(self, response):
        user_sids = self.dbhandler.select_user_sids(self.target_ids)
        url_head = "https://api-v2.soundcloud.com/users/{0}"
        url_tail = f"/tracks?representation=&client_id={self.config['CLIENT_ID']}&limit=20&offset=0&linked_partitioning=1&app_version=1593604665&app_locale=en"
        for user_sid in user_sids:
            url = url_head.format(user_sid) + url_tail
            req = scrapy.Request(url, self.parse_track_popularity)
            yield req

    def parse_track_popularity(self, response):
        result_json = json.loads(response.body)
        collections = result_json['collection']
        if not collections:
            return
        for collection in collections:
            track_likes_count = collection['likes_count'] if collection['likes_count'] else 0
            track_playback_count = collection['playback_count'] if collection['playback_count'] else 0
            track_id = collection['id']
            self.dbhandler.update_track_popularity(track_id, track_likes_count, track_playback_count)
        if result_json['next_href']:
            url = result_json['next_href'] + f'&client_id={self.config["CLIENT_ID"]}'
            req = scrapy.Request(url, self.parse_track_popularity)
            yield req
