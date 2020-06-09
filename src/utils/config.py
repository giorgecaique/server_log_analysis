from configparser import ConfigParser
import os

ROOT_DIR = os.path.abspath(os.curdir)

class project_config:
    def get_config():
        config = ConfigParser()
        config.read('{0}/config.ini'.format(ROOT_DIR))

        return config

