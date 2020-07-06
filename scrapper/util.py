import os
from os.path import join
import json


def get_project_home():
    return os.path.abspath(os.path.dirname(__file__))


def get_config_dir():
    return join(get_project_home(), 'config')


def load_config():
    config_dir = get_config_dir()
    return json.loads(open(join(config_dir, 'config.json')).read())


def load_target_ids(target_file_name):
    target_file = open(join(get_project_home(), 'targets', target_file_name))
    target_ids = []
    for line in target_file:
        target_ids.append(line.strip())
    return target_ids


def register_gcp_credential(config):
    config = load_config()
    config_dir = get_config_dir()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = join(config_dir, config['SERVICE_ACCOUNT'])


if __name__ == '__main__':
    config = load_config()
    print(config)
