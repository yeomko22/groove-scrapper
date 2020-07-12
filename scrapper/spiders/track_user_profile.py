import warnings

import scrapy

from scrapper import util
from scrapper.dbhandler import DBHandler


class TrackUserProfileSpider(scrapy.Spider):
    name = 'track_user_profile'
    start_urls = ['https://soundcloud.com/']

    def __init__(self):
        warnings.simplefilter("ignore")
        self.config = util.load_config()
        util.register_gcp_credential(self.config)
        self.dbhandler = DBHandler(self.config)

    def parse(self, response):
        user_infos = self.dbhandler.select_all_users()
        for user_info in user_infos:
            user_tracks = self.dbhandler.select_user_tracks(user_info[0])
            for user_track in user_tracks:
                self.dbhandler.update_track_user_profile(user_track[0], user_info[1], user_info[2])
                print(user_track[0], user_info[1], user_info[2])
