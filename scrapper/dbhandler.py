import pymysql


class DBHandler:
    def __init__(self, config):
        self.conn = pymysql.connect(
            host=config['DBHOST'],
            port=config['DBPORT'],
            user=config['DBUSER'],
            passwd=config['DBPASSWORD'],
            db=config['DBNAME'],
            charset='utf8'
        )

    def insert_user(self, user_json):
        insert_sql = "insert IGNORE into users(user_id, user_sid, user_name, user_full_name, user_description, user_country, user_city) values "
        insert_sql += f"{user_json['user_id'], user_json['user_sid'], user_json['user_name'], user_json['user_full_name'], user_json['user_description'], user_json['user_country'], user_json['user_city']}"
        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql)
            self.conn.commit()

    def update_user_profile(self, user_json):
        update_sql = f"update users set user_profile_org='{user_json['user_profile_org']}', user_profile_thumbnail='{user_json['user_profile_thumbnail']}' "
        update_sql += f"where user_id='{user_json['user_id']}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def update_user_banner(self, user_json):
        update_sql = f"update users set user_banner='{user_json['user_banner']}' where user_id='{user_json['user_id']}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()
    # def select_user(self):
    #     select_sql = "select user_sid from user"
    #     with self.conn.cursor() as cursor:
    #         cursor.execute(select_sql)
    #     result = [x[0] for x in cursor.fetchall()]
    #     return result
    #
    # def insert_tracks(self, tracks):
    #     insert_sql = "insert IGNORE into tracks(track_id, track_title, track_user_sid, track_artwork_link, track_genre, track_m3u8_url) values "
    #     for track in tracks:
    #         insert_sql += f"{track['track_id'], track['track_title'], track['track_user_sid'], track['track_artwork_link'], track['track_genre'], track['track_m3u8_url']}, "
    #     insert_sql = insert_sql[:-2]
    #     with self.conn.cursor() as cursor:
    #         cursor.execute(insert_sql)
    #         self.conn.commit()
