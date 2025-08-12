import pathlib


DIR = pathlib.Path(__file__).parent.parent.resolve()

DATASOURCE_FILE = DIR / 'datasource.csv'
LOG_FILE = DIR / 'python' / '.log'

LOG_FORMAT = '[%(levelname)s] [%(name)s] [%(asctime)s] %(message)s'
