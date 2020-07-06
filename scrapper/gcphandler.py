from google.cloud import storage
import os

class GCPHandler:
  def __init__(self, config):
    self.storage_client = storage.Client()
    self.bucket_name = config['BUCKET']
    self.bucket = self.storage_client.bucket(self.bucket_name)

  def upload_file(self, src_path, dst_path):
    blob = self.bucket.blob(dst_path)
    blob.upload_from_filename(src_path)
    blob.make_public()
    return blob.public_url
