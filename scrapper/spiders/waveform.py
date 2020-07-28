"""
1. 디비에서 트랙 아이디를 가져온다.
2. 트랙 API를 요청해서 waveform url를 받아온다.
3. waveform json을 받아와서 이를 자체 디비에 저장한다.
"""

import json
import warnings

import scrapy

from scrapper import util
from scrapper.dbhandler import DBHandler
from scrapper.gcphandler import GCPHandler


class WaveformSpider(scrapy.Spider):
    name = 'waveform'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.target_ids = util.load_target_ids('target.txt')
        self.dbhandler = DBHandler(self.config)
        self.gcphandler = GCPHandler(self.config)
        warnings.filterwarnings(action='ignore')

    def parse(self, response):
        track_infos = self.dbhandler.select_all_track_ids()
        for track_info in track_infos:
            track_id = track_info[0]
            url = f'https://api-v2.soundcloud.com/tracks/{track_id}?client_id={self.config["CLIENT_ID"]}'
            req = scrapy.Request(url, self.parse_track)
            req.meta['track_id'] = track_id
            yield req

    def parse_track(self, response):
        track_id = response.meta['track_id']
        track_json = json.loads(response.body)
        new_url = track_json['waveform_url']
        req = scrapy.Request(url=new_url, callback=self.parse_waveform)
        req.meta['track_id'] = track_id
        yield req

    def parse_waveform(self, response):
        track_id = response.meta['track_id']
        waveform = json.loads(response.body)
        self.dbhandler.insert_waveform(track_id, waveform)
