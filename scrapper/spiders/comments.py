'''
1. targets 폴더 안에 미리 아티스트 id를 적어놓은 텍스트 파일을 읽어옴
2. 각 아티스트별 트랙 id를 가져옴
3. 트랙별 댓글 첫 페이지를 요청한 다음, 그 안에 실려있는 url을 재귀적으로 요청함으로써 댓글 데이터 수집
'''

import json

import scrapy

from scrapper import util
from scrapper.dbhandler import DBHandler
from scrapper.gcphandler import GCPHandler
import warnings
from PIL import Image
import os
import io

class CommentsSpider(scrapy.Spider):
    name = 'comment'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.target_ids = util.load_target_ids('target.txt')
        self.dbhandler = DBHandler(self.config)
        self.gcphandler = GCPHandler(self.config)
        warnings.filterwarnings(action='ignore')

    def parse(self, response):
        for target_id in self.target_ids:
            track_infos = self.dbhandler.select_track_ids(target_id)
            for track_info in track_infos:
                track_id = track_info[0]
                url = f'https://api-v2.soundcloud.com/tracks/{track_id}/comments?filter_replies=0&threaded=1&client_id={self.config["CLIENT_ID"]}&offset=0&limit=20&app_version=1595511948&app_locale=en'
                req = scrapy.Request(url, self.parse_comment)
                req.meta['cnt'] = 0
                req.meta['user_id'] = target_id
                req.meta['track_id'] = track_id
                yield req

    def parse_comment(self, response):
        cnt = response.meta['cnt']
        if cnt > 5:
            return
        user_id = response.meta['user_id']
        track_id = response.meta['track_id']
        comment_json = json.loads(response.body)
        collections = comment_json['collection']
        next_href = comment_json['next_href']
        comments = []
        for collection in collections:
            comments.append({
                "created_at": self.trim_created_at(collection['created_at']),
                "comment_user_id": user_id,
                "comment_uploader_id": collection['user']['permalink'],
                "comment_track_id": track_id,
                "comment_body": collection['body'] if collection['body'] else '',
            })
            user_profile_link = collection['user']['avatar_url']
            user_profile_link = user_profile_link.replace('large', 't500x500') if user_profile_link else ''
            user_json = {
                "user_id": collection['user']['permalink'],
                "user_sid": collection['user']['id'],
                "user_name": collection['user']['username'] if collection['user']['username'] else '',
                "user_full_name": collection['user']['full_name'] if collection['user']['full_name'] else '',
                "user_description": '',
                "user_country": collection['user']['country_code'] if collection['user']['country_code'] else '',
                "user_city": collection['user']['city'] if collection['user']['city'] else '',
                "user_type": 1
            }
            self.dbhandler.insert_user(user_json)
            if user_profile_link:
                user_profile_req = scrapy.Request(user_profile_link, self.parse_profile_img)
                user_profile_req.meta['user_json'] = user_json
                yield user_profile_req
        self.dbhandler.insert_comments(comments)
        if next_href:
            req = scrapy.Request(url=next_href, callback=self.parse_comment)
            req.meta['cnt'] = cnt + 1
            req.meta['user_id'] = user_id
            req.meta['track_id'] = track_id
            yield req

    def parse_profile_img(self, response):
        user_json = response.meta['user_json']
        image = Image.open(io.BytesIO(response.body))
        profile_name = f'{user_json["user_id"]}_profile_500x500.jpg'
        profile_thumnail_name = f'{user_json["user_id"]}_profile_128x128.jpg'
        image.save(f'./tmp/{profile_name}')
        image = image.resize((128, 128))
        image.save(f'./tmp/{profile_thumnail_name}')
        profile_url = self.gcphandler.upload_file(f'./tmp/{profile_name}', f'users/profiles/org/{user_json["user_id"]}.jpg')
        profile_thumbnail_url = self.gcphandler.upload_file(f'./tmp/{profile_thumnail_name}', f'users/profiles/thumbnail/{user_json["user_id"]}.jpg')
        user_json["user_profile_org"] = profile_url
        user_json["user_profile_thumbnail"] = profile_thumbnail_url
        os.remove(f'./tmp/{profile_name}')
        os.remove(f'./tmp/{profile_thumnail_name}')
        self.dbhandler.update_user_profile(user_json)

    def trim_created_at(self, created_at_str):
        created_at_str = created_at_str.replace('T', ' ')
        created_at_str = created_at_str.replace('Z', '')
        return created_at_str
