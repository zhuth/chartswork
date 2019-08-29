import requests
import json
import pymongo
import .amap_industries

class AmapGeoProvider:

    def __init__(self, apikey, buffer_server=None):
        self.apikey = apikey
        if isinstance(buffer_server, str): buffer_server = {'host': buffer_server}
        self.buffer_server = buffer_server

    def _connect_buffer(self) -> (pymongo.MongoClient, None):
        client = pymongo.MongoClient(**self.buffer_server)['amap_geo_buffer']
        return client

    def _read_buffer(self, **kwargs) -> (dict, None):
        if self.buffer_server is None: return
        return self._connect_buffer().find_one(kwargs)

    def _write_buffer(self, **kwargs) -> str:
        if self.buffer_server is None: return
        return str(self._connect_buffer().insert_one(kwargs))

    def request_url(self, act, **params) -> dict:
        params['key'] = self.apikey
        url = f'https://restapi.amap.com/v3/{act}'
        j = json.loads(requests.get(url, params=params).content)
        return j

    def geocode(self, city, address_or_addresses : (list, str)) -> list:
        addrs = address_or_addresses if isinstance(address_or_addresses, list) else [address_or_addresses]
        r = {}
        a = list(set(addrs))

        for addr in a:
            buffered = (self._read_buffer(addr=addr) or {}).get('result')
            if buffered: r[addr] = buffered

        for addr in r:
            a.remove(addr)

        batch = len(a) > 1

        for i in range(0, len(a), 10):
            addrstr = '|'.join(a[i:i+10])
            j = self.request_url('geocode/geo', batch=batch, city=city, address=addrstr)
            for ad, g in zip(a[i:i+10], j['geocodes']):
                g['location'] = self.wgs_coordinate(g['location'])
                r[ad] = g

        for addr in a:
            self._write_buffer(addr=addr, result=r[addr])

        return [
            (ad, r[ad]) for ad in addrs
        ]

    def wgs_coordinate(gcj=None, bd=None):
        
        assert (gcj is None) != (bd is None), 'Must specify one and only one of :gcj: or :bd:'

        def bd2gcj(lon, lat):
            x_pi = math.pi * 3000.0 / 180.0
            x = lon - 0.0065
            y = lat - 0.006
            z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi)
            theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
            gg_lon = z * math.cos(theta)
            gg_lat = z * math.sin(theta)
            return [ gg_lon, gg_lat ]
            
        from geojson_utils import gcj02towgs84 as gcj2wgs 
            
        def bd2wgs(lon, lat):
            return gcj2wgs(*bd2gcj(lon, lat))

        def f(s):
            if isinstance(s, str):
                return [float(_) for _ in s.split(',')]
            else:
                return s

        if bd:
            return bd2wgs(*f(bd))
        
        if gcj:
            return gcj2wgs(*f(gcj))

    def get_city_codes(self, city_or_cities):
        from .amap_cities import get_city_codes
        return get_city_codes(city_or_cities)

    def amap_poi(self, city, category) -> list: # geojson

        city = self.get_city_codes(city)[0]

        params = {
            'citylimit': True,
            'city': citycode,
            'types': category + '00',
            'page': 1
        }

        print(citycode, category)

        pages = 1
        fs = (self._read_buffer(city=citycode, category=category) or {}).get('fs', [])

        if fs:
            return fs

        i = 0
        while i < pages:
            try:
                print(len(fs), '{}/{}'.format(i, pages))
                params['page'] = i
                time.sleep(0.1)
                
                j = self.request_url('place/text', **params)
                
                fs += [{
                    'type': 'Feature',
                    'geometry': {
                        'type':'Point',
                        'coordinates': [
                            self.wgs_coordinate(r['location'])
                        ]},
                    'properties': {
                        'name':r['name'],
                        'region': citycode,
                        'biz':r['typecode'] + ' ' + r['type']
                    }
                } for r in j['pois']]
                i += 1
                pages = int(math.ceil(int(j['count']) / 20))
            except:
                pass
        return fs
