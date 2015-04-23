import configparser
import os

home = os.path.expanduser("~")
default = os.path.join(home, ".config", "dbtracker.ini")


def conf_to_dict(config, provider):
    conf_dict = {
        "host": config[provider]['host'],
    }


def read_config(file=default):
    config = configparser.ConfigParser()
    config.read(file)
    return config
