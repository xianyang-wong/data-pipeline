import os
import configparser


def get_config(path=None):
    if not path:
        module_dir = os.path.dirname(__file__)
        path = os.path.join(module_dir, '../config/config.ini')
    config = configparser.ConfigParser()
    config.read(path)
    return config


def set_app_path(path):
    global app_path
    app_path = path


def get_app_path():
    return app_path