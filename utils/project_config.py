from configparser import ConfigParser
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class project_config:
    def get_config():
        config = ConfigParser()
        print(ROOT_DIR)
        config.read('{0}/config.ini'.format(ROOT_DIR))

        return config
