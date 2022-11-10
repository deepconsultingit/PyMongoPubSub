import logging
import os
from logging import config

DEFAULT_LOGGING_FILE = "pubsub-logging.conf"
DEFAULT_LOGGING_ENV_KEY = "PUBSUBLOG"

global logConfigured
__logConfigured = False

global logConf
__logConf = {
        "version": 1,
        "root": {
            "handlers": ["console"],
            "level": "DEBUG"
        },
        "handlers":{
            "console": {
                "formatter": "std_out",
                "class": "logging.StreamHandler",
                "level": "DEBUG"
            }
        },
        "formatters": {
            "std_out": {
                "format": "%(asctime)s : %(levelname)6s : %(module)s : %(name)s : %(funcName)s [%(lineno)5d] # %(message)s",
                "datefmt": "%y-%m-%dT%H:%M:%S"
            }
        },
    }


def logConfiguration():
    global __logConfigured
    global __logConf

    if not __logConfigured:
        config.dictConfig(__logConf)

        logFileConf = os.getenv(DEFAULT_LOGGING_ENV_KEY)
        if logFileConf is None:
            logFileConf = DEFAULT_LOGGING_FILE

        if os.path.exists(logFileConf) and os.path.isfile(logFileConf):
            config.fileConfig(logFileConf, None, True)

        __logConfigured = True
