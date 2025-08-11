import logging
import pathlib
import sys


DIR = pathlib.Path(__file__).parent.parent.resolve()

DATASOURCE_FILE = DIR / 'datasource.csv'
LOG_FILE = DIR / 'python' / '.log'

LOG_FORMAT = '[%(levelname)s] [%(name)s] [%(asctime)s] %(message)s'


def get_logger(name: str = None) -> logging.Logger:
    logger_formatter = logging.Formatter(LOG_FORMAT)

    logger_file_handler = logging.StreamHandler(LOG_FILE.open('a'))
    logger_file_handler.setLevel(logging.DEBUG)
    logger_file_handler.setFormatter(logger_formatter)

    logger_stream_handler = logging.StreamHandler(sys.stdout)
    logger_stream_handler.setLevel(logging.DEBUG)
    logger_stream_handler.setFormatter(logger_formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logger_file_handler)
    logger.addHandler(logger_stream_handler)

    return logger
