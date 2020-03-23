"""
Package storing common functions getting used by multiple other packages
"""

from datetime import date

COLLISION = 'collision'

TIME = 'time'

LOCATION = 'location'

HOUR = 'hour'
DAY = 'day'
MONTH = 'month'
YEAR = 'year'
ON_STREET = 'on_street'
OFF_STREET = 'off_street'
CROSS_STREET = 'cross_street'

MONDAY = 'mon'
TUESDAY = 'tue'
WEDNESDAY = 'wed'
THURSDAY = 'thu'
FRIDAY = 'fri'
SATURDAY = 'sat'
SUNDAY = 'sun'

JAN = 'jan'
FEB = 'feb'
MAR = 'mar'
APR = 'apr'
MAY = 'may'
JUN = 'jun'
JUL = 'jul'
AUG = 'aug'
SEP = 'sep'
OCT = 'oct'
NOV = 'nov'
DEC = 'dec'

CURRENT_YEAR = date.today().year
START_YEAR = 2012

SAFEST = 'safest'
DANGEROUS = 'dangerous'

SEPARATOR = '_'


def get_key(group, city_code, identifier, value):
    key = COLLISION + SEPARATOR + city_code + SEPARATOR + group + SEPARATOR + identifier + SEPARATOR + str(value)
    return str(key)


def get_year(dt_year):
    return dt_year


def get_month(dt_month):
    months = {1: JAN, 2: FEB, 3: MAR, 4: APR, 5: MAY, 6: JUN, 7: JUL,
              8: AUG, 9: SEP, 10: OCT, 11: NOV, 12: DEC}
    return months[dt_month]


def get_day(dt_day):
    days = {0: MONDAY, 1: TUESDAY, 2: WEDNESDAY, 3: THURSDAY, 4: FRIDAY,
            5: SATURDAY, 6: SUNDAY}
    return days[dt_day]


def get_hour(dt_hour):
    return dt_hour

#Weather types for geohash buckets
weather_type = ['clear sky',
'few clouds',
'scattered clouds',
'broken clouds',
'shower rain',
'rain',
'thunderstorm',
'snow',
'mist',
'Thunderstorm',
'thunderstorm with light rain',
'thunderstorm with rain',
'thunderstorm with heavy rain',	 
'light thunderstorm',
'thunderstorm',	
'heavy thunderstorm',	 
'ragged thunderstorm',	 
'thunderstorm with light drizzle',	 
'thunderstorm with drizzle',	
'thunderstorm with heavy drizzle',	
'Drizzle',
'light intensity drizzle',	 
'drizzle',	 
'heavy intensity drizzle',	 
'light intensity drizzle rain',	 
'drizzle rain',	 
'heavy intensity drizzle rain',	 
'shower rain and drizzle',
'heavy shower rain and drizzle',	 
'shower drizzle',	 
'Rain',
'light rain',	 
'moderate rain',	
'heavy intensity rain',	
'very heavy rain',	
'extreme rain',	 
'freezing rain',	
'light intensity shower rain',	 
'shower rain', 
'heavy intensity shower rain',	
'ragged shower rain',	
'Snow',
'light snow',	
'Heavy snow',	 
'Sleet',	
'Light shower sleet',	 
'Shower sleet',	
'Light rain and snow',	 
'Rain and snow',	
'Light shower snow',	
'Shower snow',	 
'Heavy shower snow',	
'Atmosphere',
'mist',	 
'Smoke',	
'Haze',	
'Dust',	
'sand/ dust whirls',	 
'Fog',	
'fog',	 
'Sand',	
'sand',	 
'Dust',	
'dust',	
'Ash',
'volcanic ash',	 
'Squall',	
'squalls',	 
'Tornado',
'tornado',	
'Clear',
'clear sky',	
'Clouds',
'few clouds: 11-25%',	 
'scattered clouds: 25-50%',	
'broken clouds: 51-84%',	
'overcast clouds: 85-100%'
]
