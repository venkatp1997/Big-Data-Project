'''
This file takes as arguments: the accidents data for a place, the name of the place itself. (NYC, LA, CHI).
Example run: python3 accidents_final.py Motor_Vehicle_Collisions_-_Crashes.csv NYC
'''

import pandas as pd
from dateutil import tz
from datetime import datetime
import redis
import json
import sys

# Key format for weather DB.
def get_key_weather(key, place):
    return "weather_" + place + "_" + str(key)

# Key format for accident DB.
def get_key_accident(key, place):
    return "accident_" + place + "_" + str(key)

# Read CSV file into Pandas Data Frame.
data = pd.read_csv(sys.argv[1])

# The second argument is the place (either 'NYC', 'LA' or 'CHI')
place = sys.argv[2]

# Get DB indices from sys arguments.
db_index_to_read = int(sys.argv[3])
db_index_to_write = int(sys.argv[4])

# Launch connector to redis.
r1 = redis.Redis(host='localhost', port=6379, db=db_index_to_read, decode_responses=True)
r2 = redis.Redis(host='localhost', port=6379, db=db_index_to_write, decode_responses=True)

# Iterate over dataset.
for index, row in data.iterrows():
    # Fetch date and time of accident.
    try:
        time_date = row['CRASH DATE'] + " " + row['CRASH TIME']
    except:
        continue

    # Define format of time, from and to timezones for epoch time conversion.
    time_format = '%m/%d/%Y %H:%M'
    from_zone = tz.gettz('America/New_York')
    to_zone = tz.gettz('UTC')

    # Get datetime object.
    time_obj = datetime.strptime(time_date, time_format).replace(tzinfo=from_zone).astimezone(to_zone)
    
    # Check for errors.
    if time_date is None or r1.exists(get_key_weather(time_obj.date(), place)) == False:
        continue
    
    # Pick the closest weather measurement we have for accident time. 
    # For example, if accident happened at 1.00PM we want to pick the weather measured at 1.30PM instead of the weather measured at 11AM.

    # Fetch the weather data for the date the accident happened.
    # This is present in the Redis DB because we inserted these records in the previous step. 
    weather_data = json.loads(r1.get(get_key_weather(time_obj.date(), place)))
    min_diff = 1e9
    min_item = ""
    
    for item_raw in weather_data:
        item = json.loads(item_raw)

        # Check whether the time this weather was measured is the closer to accident time than what we have currently.
        if abs(item['time'] - time_obj.timestamp()) < min_diff:
            min_diff = abs(item['time'] - time_obj.timestamp())
            min_item = item

    # Insert into Redis.
    r2.set(get_key_accident(index, place), json.dumps(min_item))

    # Tracking progress of how much we have inserted into Redis.
    if (index % 10000) == 0:
        print (index)
