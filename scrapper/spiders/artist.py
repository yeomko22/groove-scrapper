'''
1. targets 폴더 안에 미리 아티스트 id를 적어놓은 텍스트 파일을 읽어옴
2. 각 아티스트별 상세 페이지를 요청하여 아티스트 상세 정보 수집
3. 유저 프로필 이미지 수집 및 썸네일 생성
4. 유저 배너 이미지 수집
'''

import io
import json
import os

import scrapy
from PIL import Image
from bs4 import BeautifulSoup

from scrapper import util
from scrapper.dbhandler import DBHandler
from scrapper.gcphandler import GCPHandler


class ArtistSpider(scrapy.Spider):
    name = 'artist'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.target_ids = util.load_target_ids('target.txt')
        self.dbhandler = DBHandler(self.config)
        self.gcphandler = GCPHandler(self.config)

    def parse(self, response):
        for target_id in self.target_ids:
            artist_url = f'https://soundcloud.com/{target_id}'
            yield scrapy.Request(artist_url, self.parse_userpage)

    def parse_userpage(self, response):
        userpage_soup = BeautifulSoup(response.body, 'lxml')
        script = str(userpage_soup.find_all('script')[-1])
        script = script.split('"data":[')[-1].replace(']}]);</script>', '')
        user_info = json.loads(script)
        user_profile_link = user_info['avatar_url']
        user_profile_link = user_profile_link.replace('large', 't500x500') if user_profile_link else ''
        user_banner_link = user_info['visuals']
        user_banner_link = user_banner_link['visuals'][0]['visual_url'] if user_banner_link else ''
        user_json = {
            "user_id": user_info['permalink'],
            "user_sid": user_info['id'],
            "user_name": user_info['username'] if user_info['username'] else '',
            "user_full_name": user_info['full_name'] if user_info['full_name'] else '',
            "user_description": user_info['description'] if user_info['description'] else '',
            "user_country": user_info['country_code'] if user_info['country_code'] else '',
            "user_city": user_info['city'] if user_info['city'] else '',
        }
        self.dbhandler.insert_user(user_json)
        if user_profile_link:
            user_profile_req = scrapy.Request(user_profile_link, self.parse_profile_img)
            user_profile_req.meta['user_json'] = user_json
            yield user_profile_req
        if user_banner_link:
            user_banner_req = scrapy.Request(user_banner_link, self.parse_banner_img)
            user_banner_req.meta['user_json'] = user_json
            yield user_banner_req

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

    def parse_banner_img(self, response):
        user_json = response.meta['user_json']
        image = Image.open(io.BytesIO(response.body))
        banner_name = f'{user_json["user_id"]}_banner.jpg'
        image.save(f'./tmp/{banner_name}')
        banner_url = self.gcphandler.upload_file(f'./tmp/{banner_name}', f'users/banners/{user_json["user_id"]}.jpg')
        user_json["user_banner"] = banner_url
        self.dbhandler.update_user_banner(user_json)
        os.remove(f'./tmp/{banner_name}')
