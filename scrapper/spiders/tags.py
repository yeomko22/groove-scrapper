'''
1. targets 폴더 안에 미리 아티스트 id를 적어놓은 텍스트 파일을 읽어옴
2. 각 아티스트별 트랙 id 값을 읽어옴
3. 트랙별 상세 페이지를 요청 후, 태그 정보 파싱, 디비 저장
'''

import json
import re

import scrapy
from bs4 import BeautifulSoup

from scrapper import util
from scrapper.dbhandler import DBHandler


class TagSpider(scrapy.Spider):
    name = 'tag'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.target_ids = util.load_target_ids('target01.txt')
        self.dbhandler = DBHandler(self.config)
        self.user_sids = self.dbhandler.select_user_sids(self.target_ids)
        self.re_quoted = re.compile(r'\"(.+?)\"')

    def parse(self, response):
        for user_sid in self.user_sids:
            track_infos = self.dbhandler.select_track_permanlink(user_sid)
            for track_id, track_permalink in track_infos:
                track_req = scrapy.Request(track_permalink, self.parse_track_page)
                track_req.meta['track_id'] = track_id
                yield track_req

    def parse_track_page(self, response):
        track_id = response.meta['track_id']
        trackpage_soup = BeautifulSoup(response.body, 'lxml')
        script = str(trackpage_soup.find_all('script')[-1])
        script = script.split('"data":[')[-1].replace(']}]);</script>', '')
        track_info = json.loads(script)
        tags_str = track_info['tag_list']
        quoted_tags = re.findall(self.re_quoted, tags_str)
        simple_tags = [x for x in re.sub(self.re_quoted, '', tags_str).split(' ') if x]
        tags = quoted_tags + simple_tags
        if tags:
            self.dbhandler.insert_tags(track_id, tags)
