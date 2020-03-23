import redis
import utils
import sys
import geohash
import json
import utils

'''
This file reads weather pattern from by successively reducing the precision and returns the most frequent probable cause of accidents
for that precision. Otherwise, returns the stats for entire city.

Run Command : python3 latlong_get.py <city_code> <db_to_read> lat lon
Sample Run command : python3 latlong_get.py NYC 3 40.6970020 -73.9360100

'''

def make_hash(lat,lon):
    h = geohash.encode(float(lat),float(lon))
    return str(h)

#Create various precision levels in hash
def create_levels(s):
    ret = []
    ret.append(s)
    for i in range(1,len(s)):
        ret.append(s[:-i])
    return ret

def get_key_pattern(h,city_code):
    return 'accident_'+city_code+'_'+h

def get_total_key(city_code):
    return 'accident_'+city_code+'_total_*'

def get_weather(key):
    key = key.split('_')
    ret = ""
    for i in range(3,len(key)):
        ret += key[i]+" "
    ret = ret.strip()
    return ret

def do_query2(key_pattern):
    ret = {}
    for key in r.scan_iter(key_pattern):
        weather_type = get_weather(key)
        ret[int(r.get(key))] = weather_type
    new_ret =sorted(ret.items(), key=lambda x: x[0], reverse=True)
    return new_ret

def get_buckets(q):
    ret = []
    for w in utils.weather_type:
        weather = w.replace(' ','_').strip()
        k = q +'_' + weather 
        ret.append(k)
    return ret

def do_query(keys):
    ret = {}
    for key in keys:
        if(r.get(key)):
            weather_type = get_weather(key)
            ret[int(r.get(key))] = weather_type
    new_ret = sorted(ret.items(), key=lambda x: x[0], reverse=True)
    return new_ret
         

def find_nearest_weather(query_patterns,city_code):
    for q in query_patterns:
        print("Searching in the precision of....->",q)
        query = get_key_pattern(q, city_code)
        keys = get_buckets(query)
        results = do_query(keys)
        if results is not None and len(results) > 0:
            return results
        else:
            continue
    
    return do_query(get_total_key(city_code))
        
        
if __name__ == "__main__":
    
    city_code = sys.argv[1] #'NYC'
    db_index_to_read = sys.argv[2]
    lat = sys.argv[3]
    lon = sys.argv[4]
    
    r = redis.Redis(host='localhost', port=6379, db=db_index_to_read, decode_responses=True)
    ll = [float(lat),float(lon)]
    #ll = ['40.6970020','-73.9360100']
    query_hash = make_hash(ll[0],ll[1])
    query_patterns = create_levels(query_hash)
    
    weather_data = find_nearest_weather(query_patterns,city_code)

    if weather_data is not None and len(weather_data) > 0:
        print(weather_data[0][1]," weather pattern caused most accidents with frequency:",weather_data[0][0])
    else:
        print("Not Found")


    '''
    python3 latlong_get.py NYC 3 40.7219510525 -73.99626933038
    '''





    
