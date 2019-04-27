import os

environ_variables = dict(os.environ)

SAVE_TARGETS = {"PARSER":"parser","SCRAPER":"scraper"}
EFS_ORIGINS = SAVE_TARGETS
EFS_ORIGINS['CRAWLER'] = EFS_ORIGINS['SCRAPER']

QUERY_VERSIONS = {}

Defaults = {"TIMESTAMP_FORMAT":'%Y-%m-%d--%H:%M:%S', "VERSION_SEPARATOR":"#@#"}