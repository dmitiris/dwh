from json import loads

from .meta import Meta

try:
    with open('config.json', 'r') as fp:
        CONFIG = loads(fp.read())
except FileNotFoundError:
    CONFIG = {}

DBUSER = CONFIG.get('DBUSER')
DBWORD = CONFIG.get('DBWORD')
DRIVER = CONFIG.get('DRIVER')
CONN_STR = CONFIG.get('CONN_STR')
JDBC_JAR = CONFIG.get('JDBC_JAR')
FILES_SOURCE_DIR = CONFIG.get('FILES_SOURCE_DIR', 'data')
FILES_BACKUP_DIR = CONFIG.get('FILES_BACKUP_DIR', 'archive')
SQL_SCRIPTS = CONFIG.get("SQL_SCRIPTS", 'sql_scripts')
OUTPUT = []
META = Meta(**CONFIG.get('META_TABLE'))
