import pymongo
from sqlalchemy import create_engine

from helpers import GlobalSettings


class DataTransformer:

    def transform(self, ds):
        return ds


class GeoTransformer(DataTransformer):

    LONLAT = 'location'
    FORMATTED_ADDR = 'formatted_address'
    RAW = ''
    
    from .geo import AmapGeoProvider as _amap

    def __init__(self, city, src_field, dst_field=None, method='Address'):
        if dst_field is None: dst_field = src_field
        self.city = city
        self.src_field = src_field
        self.dst_field = dst_field
        self.method = method

    def transform(self, ds):
        amap = GeoTransformer._amap(**GlobalSettings['amap'])
        for r in ds:
            g = amap.geocode(self.city, r[self.src_field])
            if self.method != GeoTransformer.RAW:
                g = g[self.method]
            r[self.dst_field] = g
        return ds
