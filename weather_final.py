import json
import redis
import datetime
import sys

# Key format for weather DB.
def get_key(key, place):
    return "weather_" + place + "_" + str(key)

# Load JSON File 
with open(sys.argv[1]) as f:
    data = json.load(f)

# The second argument is the place (either 'NYC', 'LA' or 'CHI')
place = sys.argv[2]
db_index = int(sys.argv[3])

# Connect to redis.
r = redis.Redis(host='localhost', port=6379, db=db_index, decode_responses=True)

# Iterate over the JSON array.
for item in data:
    # Convert epoch time to datetime object.
    t1 = datetime.datetime.utcfromtimestamp(item['dt'])

    # Get only the date from the datetime object.
    key = t1.date()

    # Definte empty JSON Array.
    val = json.loads("[]")

    # If value already exists, retrieve JSON array for that date.
    if(r.exists(get_key(key, place))):
        val = json.loads(r.get(get_key(key, place)))
   
    # Append new data to existing JSON array if exists/new JSON array.
    new_json_data = {
        'temperature': item['main']['temp'], 
        'pressure': item['main']['pressure'],
        'time': item['dt'],
        'weather': item['weather']
    }
    
    x = json.dumps(new_json_data)
    val.append(x)

    # Insert into Redis DB. 
    r.set(get_key(key, place), json.dumps(val))

