import yaml
import glob
import os
import sys
import pandas as pd
import json

import pymongo
import sqlalchemy

# Load settings from configs dir

GlobalSettings = {}

for ya in glob.glob('configs/*.yaml'):
    yk = ya.split('/')[-1].split('.')[0]
    with open(ya) as yf:
        GlobalSettings[yk] = yaml.safe_load(yf.read())

# Prepare data sources

DataSources = {
    '.csv': lambda fupload, **kwargs: pd.read_csv(fupload, **kwargs),
    '.xlsx': lambda fupload, **kwargs: pd.read_excel(fupload, **kwargs)
}

for source_name, settings in GlobalSettings['datasources'].items():
    atype = settings['type']
    if atype == 'sql':
        engine = sqlalchemy.create_engine(settings['uri'])
        DataSources[source_name] = lambda sql, **kwargs: pd.read_sql_query(sql, engine, **kwargs)
    elif atype == 'mongo':
        client = pymongo.MongoClient(settings['uri'])

        def _read_mongo(qstr, client):
            if isinstance(qstr, str): qstr = json.loads(qstr)
            coll = qstr['__collection']
            del qstr['__collection']
            return pd.DataFrame(list(client[coll].find(qstr)))        
        DataSources[source_name] = lambda q, **kwargs: _read_mongo(q, client, **kwargs)
    