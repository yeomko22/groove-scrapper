"""
1. target 텍스트 파일 안의 아티스트들의 sid를 디비에서 읽어
2. 각 아티스트별로 track 정보를 수집
3. 각 트랙별 아트웤 이미지 파일 수집
4. 각 트랙별 m3u8 파일 수집
"""
import io
import json
import os

import scrapy
from PIL import Image

from scrapper import util
from scrapper.dbhandler import DBHandler
from scrapper.gcphandler import GCPHandler
import warnings
import requests


class TrackColorSpider(scrapy.Spider):
    name = 'track_color'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        warnings.simplefilter("ignore")
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.target_ids = util.load_target_ids('target.txt')
        self.dbhandler = DBHandler(self.config)
        self.gcphandler = GCPHandler(self.config)

    def parse(self, response):
        track_infos = self.dbhandler.select_all_track_ids_thumbnail()
        for track_id, track_artwork_thumbnail in track_infos:
            if not track_artwork_thumbnail:
                continue
            track_artwork_thumbnail = track_artwork_thumbnail.replace('https://storage.googleapis.com/', 'gs://')
            headers = {'Authorization': f'Bearer {self.config["VISION_TOKEN"]}',
                       'Content-Type': 'application/json; charset=utf-8'}
            payload = {
              "requests": [
                {
                  "image": {
                    "source": {
                      "gcsImageUri": track_artwork_thumbnail
                    }
                  },
                  "features": [
                    {
                      "maxResults": 1,
                      "type": "IMAGE_PROPERTIES"
                    }
                  ]
                }
              ]
            }
            response = requests.post(url='https://vision.googleapis.com/v1/images:annotate',
                                     data=json.dumps(payload),
                                     headers=headers)
            try:
                color_info = response.json()
                color_info = color_info['responses'][0]['imagePropertiesAnnotation']['dominantColors']['colors'][0]['color']
                track_color = f'{color_info["red"]}|{color_info["green"]}|{color_info["blue"]}'
                self.dbhandler.update_track_color(track_id, track_color)
            except:
                print('exception: ', track_artwork_thumbnail)
