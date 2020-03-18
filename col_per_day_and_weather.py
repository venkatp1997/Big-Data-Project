import json
import redis
from datetime import datetime
import pytz
import dateutil.parser
import sys
import pandas as pd
from itertools import zip_longest
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None  # default='warn'

r = redis.Redis(host='localhost', port=6379, db=0)
filename = sys.argv[1]
i = 0
accident_weather = {}
q = ''
for key in r.scan_iter("accident*"):
    # delete the key
    q = r.get(key)
    accident = json.loads(q.decode())
    for i in accident['weather']:
        try:
            accident_weather[i['description']] = accident_weather[i['description']]  + 1
        except:
            accident_weather[i['description']] = 0

accident_day = {}
for key in r.scan_iter("accident*"):
    # delete the key
    q = r.get(key)
    accident = json.loads(q.decode())        
    t1 = datetime.utcfromtimestamp(accident["time"] )
    try:
        accident_day[t1] = accident_day[t1]  + 1
    except:
        accident_day[t1] = 0

accident_per_day = {} #modify the datetime object 
def func(x,y):
    try:
        accident_per_day[x] = accident_per_day[x]+ y
    except:
        accident_per_day[x] =  y
f= list(map(lambda x: func(x.strftime('%Y-%m-%d'),accident_day[x] ),accident_day))

final_days = accident_per_day
f = {k: v for k, v in sorted(final_days.items(), key=lambda item: item[1])}
day_with_most_accidents = list(f.keys())[-1]
print("the day with the most collisions")
print("collisions: " + str(accident_per_day[list(f.keys())[-1]]))
print(day_with_most_accidents)

df = pd.read_json(filename)
df["dt"] = df["dt"].apply(datetime.utcfromtimestamp)
df["dt"] = df["dt"].dt.strftime('%Y-%m-%d')
qq = df[df["dt"] == day_with_most_accidents]

def temp(x):
    return x["temp"]
def wind(x):
    return x["speed"]  
def weather(x):
    return x[0]["description"]
def humidity(x):
    return x["humidity"]
qq["temp"] = qq["main"].apply(temp)
qq["humidity"] = qq["main"].apply(humidity)
qq["wind_speed"] = qq["wind"].apply(wind)
qq["weather_description"] = qq["weather"].apply(weather)


print("average humidity "+ str(qq.humidity.mean()))
print("average temp "+ str(qq.temp.mean()))
print("average wind speed "+ str(qq.wind_speed.mean()))
print("weather descriptions during the day ")
print(qq.weather_description.value_counts())

for key,value in zip(accident_weather, accident_weather.values()):
    keyInsert = "count_collision_weather_NYC_" + key.replace(" ", "_")
    r.set(keyInsert,value)
    
    
'''
OUTPUT: 

the day with the most collisions: 2014-01-21
with 1074 collisions

average humidity 65.91666666666667
average temp -15.7825
average wind speed 2.975
weather descriptions during the day 
light snow          10
scattered clouds     5
overcast clouds      3
broken clouds        3
few clouds           2
sky is clear         1
Name: weather_description, dtype: int64

'''