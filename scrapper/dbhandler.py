import pymysql
import sys

class DBHandler:
    def __init__(self, config):
        self.conn = pymysql.connect(
            host=config['DBHOST'],
            port=config['DBPORT'],
            user=config['DBUSER'],
            passwd=config['DBPASSWORD'],
            db=config['DBNAME'],
            charset='utf8mb4'
        )

    def insert_user(self, user_json):
        insert_sql = "insert IGNORE into users(user_id, user_sid, user_name, user_full_name, user_description, user_country, user_city, user_type) values "
        insert_sql += f"{user_json['user_id'], user_json['user_sid'], user_json['user_name'], user_json['user_full_name'], user_json['user_description'], user_json['user_country'], user_json['user_city'], user_json['user_type']}"
        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql)
            self.conn.commit()

    def insert_comments(self, comments):
        if not comments:
            return
        insert_sql = "insert into comments(created_at, comment_body, comment_user_id, comment_uploader_id, comment_track_id) values "
        for comment in comments:
            insert_sql += f"{comment['created_at'], comment['comment_body'], comment['comment_user_id'], comment['comment_uploader_id'], comment['comment_track_id']}, "
        insert_sql = insert_sql[:-2]
        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql)
            self.conn.commit()

    def insert_comments_uploader(self, comments):
        if not comments:
            return
        insert_sql = "insert into comments(created_at, comment_body, comment_user_id, comment_uploader_id, comment_track_id) values "
        for comment in comments:
            insert_sql += f"{comment['created_at'], comment['comment_body'], comment['comment_user_id'], comment['comment_uploader_id'], comment['comment_track_id']}, "
        insert_sql = insert_sql[:-2]
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

    def select_all_users(self):
        select_sql = "select user_id, user_profile_org, user_profile_thumbnail from users"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return cursor.fetchall()

    def select_all_user_sids(self):
        select_sql = "select user_sid from users"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return [x[0] for x in cursor.fetchall()]

    def select_hls_per_track(self, track_id):
        select_sql = f"select track_hls from tracks where track_id='{track_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return cursor.fetchall()

    def select_user_tracks(self, user_id):
        select_sql = f"select track_id from tracks where track_user_id='{user_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return cursor.fetchall()

    def select_user_sids(self, target_ids):
        select_sql = "select user_sid from users where "
        for target_id in target_ids:
            select_sql += f"user_id='{target_id}' or "
        select_sql = select_sql[:-4]
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        result = [x[0] for x in cursor.fetchall()]
        return result

    def select_track_ids(self, user_id):
        select_sql = f"select track_id from tracks where track_user_id='{user_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return cursor.fetchall()

    def select_tracks_m3u8url(self, user_sid):
        select_sql = f"select track_id, track_m3u8_url from tracks where track_user_sid='{user_sid}'"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return cursor.fetchall()

    def select_track_nohls(self):
        select_sql = "select track_id from tracks where track_hls=''"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return cursor.fetchall()

    def select_track_permanlink(self, user_sid):
        select_sql = f"select track_id, track_permalink from tracks where track_user_sid='{user_sid}'"
        with self.conn.cursor() as cursor:
            cursor.execute(select_sql)
        return cursor.fetchall()

    def insert_tracks(self, tracks):
        insert_sql = "insert IGNORE into tracks(created_at, track_id, track_user_sid, track_title, track_genre, track_duration, track_description, track_permalink, track_likes_count, track_playback_count, track_user_id, track_user_name) values "
        for track in tracks:
            insert_sql += f"{track['created_at'], track['track_id'], track['track_user_sid'], track['track_title'], track['track_genre'], track['track_duration'], track['track_description'], track['track_permalink'], track['track_likes_count'], track['track_playback_count'], track['track_user_id'], track['track_user_name']}, "
        insert_sql = insert_sql[:-2]
        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql)
            self.conn.commit()

    def update_track_artwork(self, track_json):
        update_sql = f"update tracks set track_artwork='{track_json['track_artwork']}', track_artwork_thumbnail='{track_json['track_artwork_thumbnail']}' where track_id='{track_json['track_id']}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def update_track_user_profile(self, track_id, user_profile, user_profile_thumbnail):
        update_sql = f"update tracks set track_user_profile='{user_profile}', track_user_profile_thumbnail='{user_profile_thumbnail}' where track_id='{track_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def update_track_m3u8(self, track_json):
        update_sql = f"update tracks set track_m3u8_url='{track_json['track_m3u8_url']}' where track_id='{track_json['track_id']}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def update_track_hls(self, track_id, hlshash):
        update_sql = f"update tracks set track_hls='{hlshash}' where track_id='{track_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def update_track_permalink(self, track_id, track_permalink):
        update_sql = f"update tracks set track_permalink='{track_permalink}' where track_id='{track_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def update_track_popularity(self, track_id, track_likes_count, track_playback_cont):
        update_sql = f"update tracks set track_likes_count='{track_likes_count}', track_playback_count='{track_playback_cont}' where track_id='{track_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def update_track_artwork(self, track_id, track):
        update_sql = f"update tracks set track_artwork='{track_artwork}', track_playback_count='{track_playback_cont}' where track_id='{track_id}'"
        with self.conn.cursor() as cursor:
            cursor.execute(update_sql)
            self.conn.commit()

    def insert_tags(self, track_id, tags):
        insert_sql = 'insert into tags(tag_track_id, tag_name) values '
        for tag in tags:
            insert_sql += f'{track_id, tag}, '
        insert_sql = insert_sql[:-2]
        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql)
            self.conn.commit()
