'''
This script is used to store the collisions data in the Redis store.
The keys of the Redis store are organized to minimize the size of values and store the counts only. Logic to be processed in application layer.

Run command - python3 collision_redis_put.json  <city_code['CHI','LA','NYC']> <filename>
Eg - python3 collision_redis_put.py NYC collision_nyc.json 

3 groups of key

1. Time group
Key Pattern - 
collision_[city_id]_time_month_[jan..dec]
collision_[city_id]_time_year_xxxx
collision_[city_id]_time_day_[mon..sun]

2. Location Group
Key Pattern - 
collision_[city_id]_location_[on_street/off_street/cross_street]

3. Latlong Group
Key Pattern - 
collision_[city_id]_[lat/lon]_[collision_id]


Useful snippets -
1. To delete the db before running the script-

r.flushdb()

2. To find the counts based on patterns to answer business questions-
for key in r.scan_iter("collision_*"):
    print(key,"-->",r.get(key))
'''
import json
import redis
import ijson
from datetime import datetime
import pytz
import dateutil.parser
import sys
startTime = datetime.now()

filename = sys.argv[2]
city_code = sys.argv[1]
r = redis.Redis(host='localhost', port=6379, db=0)


#For reference
column_names = [
':sid', 
':id', 
':position', 
':created_at', 
':created_meta', 
':updated_at', 
':updated_meta', 
':meta', 
'crash_date', 
'crash_time', 
'borough', 
'zip_code', 
'latitude', 
'longitude', 
'location', 
'on_street_name', 
'off_street_name', 
'cross_street_name', 
'number_of_persons_injured', 
'number_of_persons_killed', 
'number_of_pedestrians_injured', 
'number_of_pedestrians_killed', 
'number_of_cyclist_injured', 
'number_of_cyclist_killed', 
'number_of_motorist_injured', 
'number_of_motorist_killed', 
'contributing_factor_vehicle_1', 
'contributing_factor_vehicle_2', 
'contributing_factor_vehicle_3', 
'contributing_factor_vehicle_4', 
'contributing_factor_vehicle_5', 
'collision_id', 
'vehicle_type_code1', 
'vehicle_type_code2', 
'vehicle_type_code_3', 
'vehicle_type_code_4', 
'vehicle_type_code_5' ]

cols_to_use = [
'collision_id',
'borough',
'crash_date', 
'crash_time',  
'latitude', 
'longitude',
'on_street_name',
'off_street_name',
'cross_street_name']


def get_month(dt_month):
    months = {1 : 'jan', 2 : 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun', 7: 'jul' , 8: 'aug', 9: 'sep', 10:'oct', 11:'nov', 12:'dec'}
    return months[dt_month]
    

def get_day(dt_day):
    days = {0: 'mon', 1: 'tue', 2: 'wed', 3: 'thu', 4: 'fri', 5:'sat', 6: 'sun'}
    return days[dt_day]

def get_key(group, identifier, value):
    key = "collision"+'_'+city_code+'_'+group+'_'+identifier+'_'+str(value)
    return str(key)

#Gets date and time and outputs relevant identifiers for key  
def process_datetime(date_str, time_str):
    dt = dateutil.parser.isoparse(date_str).replace(hour = int(time_str.split(':')[0]), minute = int(time_str.split(':')[1]))
    timezone = pytz.timezone('US/Eastern')
    dt = timezone.localize(dt).astimezone(pytz.utc)
    dt_year = dt.year
    dt_month = dt.month
    dt_day = dt.weekday()
    dt_hour = dt.hour
    if dt_year and isinstance(dt_year, int) and dt_month >=0 and isinstance(dt_month, int) and dt_day >= 0 and isinstance(dt_day, int) and dt_hour >=0  and isinstance(dt_hour, int):
        dt_month = get_month(dt_month)
        dt_day = get_day(dt_day)
        return [dt_year,dt_month,dt_day,dt_hour]
   
   
with open(filename, 'r') as f:
    objects = ijson.items(f, 'data.item')
    i = 0
    for row in objects:
        selected_row = []
        for item in cols_to_use:
            selected_row.append(row[column_names.index(item)])
            
   
        
        #latlon (for future use)
        collision_id = selected_row[0] #collision_id
        if collision_id and collision_id != "":
            if selected_row[4] and selected_row[4] != "" and selected_row[5] and selected_row[5] != "":
                r.set('collision_'+city_code+'_lat_' + str(collision_id) , selected_row[4])
                r.set('collision_'+city_code+'_lon_' + str(collision_id) , selected_row[5])
            
                
        
        #time
        if selected_row[2] and selected_row[2] != "" and selected_row[3] and selected_row[3] != "":
            crash_dt_arr = process_datetime(selected_row[2], selected_row[3])  #[year,month,day]
            if(crash_dt_arr != None):
                year_key = get_key("time","year",crash_dt_arr[0]) #Update year_count
                if (r.exists(year_key)): 
                    old_count = int(r.get(year_key))
                    r.set(year_key, old_count + 1)
                else:
                    r.set(year_key, 1)
                
                
                month_key = get_key("time","month",crash_dt_arr[1]) #Update month_count
                if (r.exists(month_key)): 
                    old_count = int(r.get(month_key))
                    r.set(month_key, old_count + 1)
                else:
                    r.set(month_key, 1)
                
                
                day_key = get_key("time","day",crash_dt_arr[2]) #Update day_count
                if (r.exists(day_key)): 
                    old_count = int(r.get(day_key))
                    r.set(day_key, old_count + 1)
                else:
                    r.set(day_key, 1)
                
                hour_key = get_key("time","hour",crash_dt_arr[3]) #Update hour_count
                if (r.exists(hour_key)): 
                    old_count = int(r.get(hour_key))
                    r.set(hour_key, old_count + 1)
                else:
                    r.set(hour_key, 1)
                    
            
        #loc
        if selected_row[6] or selected_row[7] or selected_row[8]:
            crash_on_street = '_'.join(selected_row[6].strip().split()) if selected_row[6] else None
            crash_off_street = '_'.join(selected_row[7].strip().split()) if selected_row[7] else None
            crash_cross_street ='_'.join(selected_row[8].strip().split()) if selected_row[8] else None
            
            
            if(crash_on_street != None):
                on_street_key = get_key("location","on_street",crash_on_street) #Update on_street_count
                if (r.exists(on_street_key)): 
                    old_count = int(r.get(on_street_key))
                    r.set(on_street_key, old_count + 1)
                else:
                    r.set(on_street_key, 1)
            
            if(crash_off_street != None):
                off_street_key = get_key("location","off_street",crash_off_street) #Update on_street_count
                if (r.exists(off_street_key)): 
                    old_count = int(r.get(off_street_key))
                    r.set(off_street_key, old_count + 1)
                else:
                    r.set(off_street_key, 1)
            
            if(crash_cross_street != None):
                cross_street_key = get_key("location","cross_street",crash_cross_street) #Update on_street_count
                if (r.exists(cross_street_key)): 
                    old_count = int(r.get(cross_street_key))
                    r.set(cross_street_key, old_count + 1)
                else:
                    r.set(cross_street_key, 1)
      

print('Process completed in-',datetime.datetime.now() - startTime)

# r.flushdb()
# for key in r.scan_iter("collision_*"):
#     print(key,"-->",r.get(key))





