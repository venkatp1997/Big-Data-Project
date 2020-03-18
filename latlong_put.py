import redis
import utils
import sys
import geohash
import json

'''
Run command - python3 latlong_put.py <db_accidents_count_read> <db_accidents_weather_read> <db_to_write>
for all accidents in accident_weather -> get collision_id, weather_event
get lat and long from collision_count db
convert to hash for a precision x. in a new store update accident_NYC_hash_<weather_type> and accident_NYC_total (cascade)


query -> get input co ordinates. convert to geohash for precision x. get from hash. if not retrieved, decrease precision and query. 
give global if all are false


'''

db_accidents_latlong = sys.argv[1] #0
db_accidents_weather = sys.argv[2] #2
db_lochash_weather = sys.argv[3] #3

r0 = redis.Redis(host='localhost', port=6379, db=db_accidents_latlong, decode_responses=True)
r2 = redis.Redis(host='localhost', port=6379, db=db_accidents_weather, decode_responses=True)
r3 = redis.Redis(host='localhost', port=6379, db=db_lochash_weather, decode_responses=True)

#Get key to be queried in counts db
def get_latlonkey(key):
    key = str(key).split('_')
    city = key[1]
    collision_id = key[2]
    lat_key = 'collision_'+str(city)+'_lat_'+str(collision_id)
    lon_key = 'collision_'+str(city)+'_lon_'+str(collision_id)
    return lat_key,lon_key,city

#Make geohash
def make_hash(lat,lon):
    h = geohash.encode(float(lat),float(lon))
    return str(h)

#Creating keys to be updated for the buckets for all the lengths of hash strings 
def create_keylist(s, city,weather_type):
    ret = []
    for i in range(1,len(s)):
        key = 'accident_'+city+'_'+s[:i]+'_'+weather_type
        ret.append(key)
    ret.append('accident_'+city+'_total_'+weather_type)
    return ret


for key in r2.scan_iter("*"):
    value = json.loads(r2.get(key))
    weather_type = str(value['weather'][0]["description"]).replace(" ","_")
    lat_key,lon_key,city = get_latlonkey(key)

    if r0.exists(lat_key) and r0.exists(lon_key):
        #print(key) Uncomment to check progress
        lat = r0.get(lat_key)
        lon = r0.get(lon_key)
        g_hash = make_hash(lat,lon)
        k_list = create_keylist(g_hash,city,weather_type)
        
        for loc_key in k_list:
            if r3.exists(loc_key):
                old_count = int(r3.get(loc_key))
                r3.set(loc_key, old_count + 1)
            else:
                r3.set(loc_key, 1)
        
    else:
        continue


    



   

