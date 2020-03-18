"""
Package for answering easy business questions

Enter command "python3 easy_question.py -h" to know about the arguments

Sample command to run the script:
python3 easy_question.py --city NYC --avg_coll_rate day --num_coll_time hour 0
                         --safe_danger_time hour safest
                         --num_coll_loc on_street WEST_211_STREET
                         --safe_danger_loc on_street safest

Here is --city is a required argument since we need to know statistics of which city are we concerned about. Rest of
arguments are not required to be passed. They happen to have default values.


OUTPUT:
Average number of collisions on dayly basis is: 458.2083333333333
Number of collisions during hour = 0 is: None
Most safest time is hour = 0 with 0 collisions
Number of collisions in on_street = WEST_211_STREET is: 2
Most safest location is on_street = ARDEN with 1 collision(s)

b'collision_NYC_time_hour_10' --> b'30339'
b'collision_NYC_time_hour_22' --> b'107864'
b'collision_NYC_time_hour_9' --> b'21152'
b'collision_NYC_time_hour_15' --> b'87216'
b'collision_NYC_time_hour_4' --> b'46435'
b'collision_NYC_time_hour_21' --> b'118939'
b'collision_NYC_time_hour_3' --> b'46413'
b'collision_NYC_time_hour_14' --> b'87628'
b'collision_NYC_time_hour_12' --> b'76717'
b'collision_NYC_time_hour_2' --> b'54807'
b'collision_NYC_time_hour_13' --> b'93738'
b'collision_NYC_time_hour_19' --> b'108621'
b'collision_NYC_time_hour_16' --> b'91929'
b'collision_NYC_time_hour_23' --> b'91722'
b'collision_NYC_time_hour_17' --> b'96272'
b'collision_NYC_time_hour_6' --> b'21808'
b'collision_NYC_time_hour_0' --> b'76193'
b'collision_NYC_time_hour_1' --> b'63784'
b'collision_NYC_time_hour_7' --> b'17736'
b'collision_NYC_time_hour_11' --> b'44580'
b'collision_NYC_time_hour_8' --> b'18886'
b'collision_NYC_time_hour_18' --> b'108525'
b'collision_NYC_time_hour_5' --> b'33278'
b'collision_NYC_time_hour_20' --> b'118671'

b'collision_NYC_time_month_aug' --> b'145569'
b'collision_NYC_time_month_mar' --> b'129186'
b'collision_NYC_time_month_feb' --> b'123281'
b'collision_NYC_time_month_oct' --> b'150908'
b'collision_NYC_time_month_nov' --> b'143818'
b'collision_NYC_time_month_may' --> b'137597'
b'collision_NYC_time_month_jan' --> b'133565'
b'collision_NYC_time_month_jun' --> b'137852'
b'collision_NYC_time_month_jul' --> b'148469'
b'collision_NYC_time_month_dec' --> b'146096'
b'collision_NYC_time_month_sep' --> b'145833'
b'collision_NYC_time_month_apr' --> b'121079'

b'collision_NYC_time_day_tue' --> b'243883'
b'collision_NYC_time_day_sun' --> b'203256'
b'collision_NYC_time_day_thu' --> b'247879'
b'collision_NYC_time_day_wed' --> b'243940'
b'collision_NYC_time_day_mon' --> b'240064'
b'collision_NYC_time_day_fri' --> b'261413'
b'collision_NYC_time_day_sat' --> b'222818'

b'collision_NYC_time_year_2015' --> b'217696'
b'collision_NYC_time_year_2013' --> b'203753'
b'collision_NYC_time_year_2016' --> b'229837'
b'collision_NYC_time_year_2018' --> b'231442'
b'collision_NYC_time_year_2020' --> b'31510'
b'collision_NYC_time_year_2017' --> b'231026'
b'collision_NYC_time_year_2012' --> b'100437'
b'collision_NYC_time_year_2014' --> b'206005'
b'collision_NYC_time_year_2019' --> b'211547'


See the arguments definition in order to know the arguments to pass and their default values (line 27 to line 39)
"""

import argparse
import redis
import utils
import sys

# Launch connector to redis.
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# define and parse the arguments passed
parser = argparse.ArgumentParser(description='Utility to answer easy business questions')

parser.add_argument('--city', default='NYC', help='city under consideration', required=True)
parser.add_argument('--avg_coll_rate', default=utils.DAY,
                    help='number of collisions hourly/daily/monthly/yearly')
parser.add_argument('--num_coll_time', nargs=2, default=[utils.HOUR, '0'],
                    help='total number of collisions in given hour/day/month/year')
parser.add_argument('--safe_danger_time', nargs=2, default=[utils.HOUR, utils.SAFEST],
                    help='safest or dangerous hour/day/month/year')
parser.add_argument('--num_coll_loc', nargs=2, default=[utils.ON_STREET, 'WEST_211_STREET'],
                    help='total number of collisions in given on/off/cross street location')
parser.add_argument('--safe_danger_loc', nargs=2, default=[utils.ON_STREET, utils.SAFEST],
                    help='safest or dangerous number on/off/cross street location')
args = parser.parse_args()

city = args.city

avg_coll_rate_type = args.avg_coll_rate
num_coll_time = args.num_coll_time
safe_danger_time = args.safe_danger_time
num_coll_loc = args.num_coll_loc
safe_danger_loc = args.safe_danger_loc


def get_avg_coll_rate():
    """
    Function to get average number of collisions on hourly/daily/monthly/yearly basis as specified
    :return:
    """
    result = 0
    if num_coll_time[0] == utils.HOUR:
        for i in range(0, 24):
            key = utils.get_key(utils.TIME, city, utils.HOUR, i)
            result = result + int(r.get(key) if r.exists(key) else 0)
        result = result / 24
    elif num_coll_time[0] == utils.DAY:
        for i in range(0, 7):
            key = utils.get_key(utils.TIME, city, utils.DAY, utils.get_day(i))
            result = result + int(r.get(key) if r.exists(key) else 0)
        result = result / 7
    elif num_coll_time[0] == utils.MONTH:
        for i in range(1, 13):
            key = utils.get_key(utils.TIME, city, utils.MONTH, utils.get_month(i))
            result = result + int(r.get(key) if r.exists(key) else 0)
        result = result / 12
    elif num_coll_time[0] == utils.YEAR:
        for i in range(utils.START_YEAR, utils.CURRENT_YEAR + 1):
            key = utils.get_key(utils.TIME, city, utils.YEAR, i)
            result = result + int(r.get(key) if r.exists(key) else 0)
        result = result / (utils.CURRENT_YEAR - utils.START_YEAR + 1)
    else:
        raise ValueError('Invalid type of duration: ', avg_coll_rate_type)

    return result


def get_num_coll_time():
    """
    Function to get number of collisions in specified duration (can be specified hour/day/month/year)
    :return:
    """
    result = 0
    if num_coll_time[0] == utils.HOUR:
        result = r.get(utils.get_key(utils.TIME, city, utils.HOUR, int(num_coll_time[1])))
    elif num_coll_time[0] == utils.DAY:
        result = r.get(utils.get_key(utils.TIME, city, utils.DAY, utils.get_day(int(num_coll_time[1]))))
    elif num_coll_time[0] == utils.MONTH:
        result = r.get(utils.get_key(utils.TIME, city, utils.MONTH, utils.get_month(int(num_coll_time[1]))))
    elif num_coll_time[0] == utils.YEAR:
        result = r.get(utils.get_key(utils.TIME, city, utils.HOUR, int(num_coll_time[1])))
    else:
        raise ValueError('Invalid type of duration: ', num_coll_time[0])

    return result


def find_extremum_time(begin, end, duration_type, extremum_type, dur_func):
    """
    Function to find duration (of duration type) during which collisions is either minimum or maximum
    :param begin:
    :param end:
    :param duration_type:
    :param extremum_type:
    :param dur_func:
    :return:
    """
    max = sys.maxsize
    min = -sys.maxsize - 1
    result = 0
    if extremum_type == utils.DANGEROUS:
        result = min
    elif extremum_type == utils.SAFEST:
        result = max
    else:
        raise ValueError('Invalid type of extremum type: ', extremum_type)
    dur = dur_func(1)
    for i in range(begin, end):
        curr_dur = dur_func(i)
        key = utils.get_key(utils.TIME, city, duration_type, curr_dur)
        curr_val = int(r.get(key) if r.exists(key) else 0)
        if extremum_type == utils.DANGEROUS:
            if curr_val > result:
                result = curr_val
                dur = curr_dur
        else:
            if curr_val < result:
                result = curr_val
                dur = curr_dur

    return dur, result


def get_safe_or_danger_time():
    """
    Function to get safest or dangerous duration (can be specified hour/day/month/year) in terms of number of collisions
    :return:
    """
    dur = 0
    result = 0
    if safe_danger_time[0] == utils.HOUR:
        begin = 0
        end = 24
        dur, result = find_extremum_time(begin, end, safe_danger_time[0], safe_danger_time[1], utils.get_hour)
    elif safe_danger_time[0] == utils.DAY:
        begin = 0
        end = 7
        dur, result = find_extremum_time(begin, end, safe_danger_time[0], safe_danger_time[1], utils.get_day)
    elif safe_danger_time[0] == utils.MONTH:
        begin = 1
        end = 13
        dur, result = find_extremum_time(begin, end, safe_danger_time[0], safe_danger_time[1], utils.get_month)
    elif safe_danger_time[0] == utils.YEAR:
        begin = utils.START_YEAR
        end = utils.CURRENT_YEAR + 1
        dur, result = find_extremum_time(begin, end, safe_danger_time[0], safe_danger_time[1], utils.get_year)
    else:
        raise ValueError('Invalid type of duration: ', safe_danger_time[0])

    return dur, result


def get_num_coll_loc():
    """
    Function to get number of collisions in specified location (can be specified off/on/cross street)
    :return:
    """
    result = 0
    if num_coll_loc[0] == utils.OFF_STREET:
        result = r.get(utils.get_key(utils.LOCATION, city, utils.OFF_STREET, num_coll_loc[1]))
    elif num_coll_loc[0] == utils.ON_STREET:
        result = r.get(utils.get_key(utils.LOCATION, city, utils.ON_STREET, num_coll_loc[1]))
    elif num_coll_loc[0] == utils.CROSS_STREET:
        result = r.get(utils.get_key(utils.LOCATION, city, utils.CROSS_STREET, num_coll_loc[1]))
    else:
        raise ValueError('Invalid type of location: ', num_coll_loc[0])

    return result


def find_extremum_loc(location_type, extremum_type):
    """
    Function to find location (of duration type) where collisions is either minimum or maximum
    :param location_type:
    :param extremum_type:
    :return:
    """
    max = sys.maxsize
    min = -sys.maxsize - 1
    result = 0
    if extremum_type == utils.DANGEROUS:
        result = min
    elif extremum_type == utils.SAFEST:
        result = max
    else:
        raise ValueError('Invalid type of extremum: ', extremum_type)
    location = ''
    for key in r.keys(
            pattern=utils.COLLISION + utils.SEPARATOR + city + utils.SEPARATOR + utils.LOCATION + utils.SEPARATOR +
                    location_type + utils.SEPARATOR + '*'):
        curr_location = key.split(sep=utils.SEPARATOR)[5]
        curr_val = int(r.get(key))
        if extremum_type == utils.DANGEROUS:
            if curr_val > result:
                result = curr_val
                location = curr_location
        else:
            if curr_val < result:
                result = curr_val
                location = curr_location

    return location, result


def get_safe_or_danger_loc():
    """
    Function to get safest or dangerous location (can be specified off/on/cross street) in terms of number of collisions
    :return:
    """
    result = 0
    location = ''
    if safe_danger_loc[0] == utils.OFF_STREET:
        location, result = find_extremum_loc(utils.OFF_STREET, safe_danger_loc[1])
    elif safe_danger_loc[0] == utils.ON_STREET:
        location, result = find_extremum_loc(utils.ON_STREET, safe_danger_loc[1])
    elif safe_danger_loc[0] == utils.CROSS_STREET:
        location, result = find_extremum_loc(utils.CROSS_STREET, safe_danger_loc[1])
    else:
        raise ValueError('Invalid type of location: ', safe_danger_loc[0])

    return location, result


if avg_coll_rate_type is not None:
    # average number of collisions in given hour/day/month
    print('Average number of collisions on {}ly basis is: {}'.format(avg_coll_rate_type, get_avg_coll_rate()))

if num_coll_time is not None and len(num_coll_time) == 2:
    # number of collisions in specified duration (can be specified hour/day/month/year)
    print('Number of collisions during {} = {} is: {}'.format(num_coll_time[0], num_coll_time[1], get_num_coll_time()))

if safe_danger_time is not None and len(safe_danger_time) == 2:
    # safest or most dangerous time (can be hour/day/month/year) in terms of number of collisions
    print('Most {} time is {} = {} with {} collision(s)'.format(safe_danger_time[1], safe_danger_time[0],
                                                              get_safe_or_danger_time()[0],
                                                              get_safe_or_danger_time()[1]))

if num_coll_loc is not None and len(num_coll_loc) == 2:
    # number of collisions in specified location (can be specified on/off/cross street)
    print('Number of collisions in {} = {} is: {}'.format(num_coll_loc[0], num_coll_loc[1], get_num_coll_loc()))

if safe_danger_loc is not None and len(safe_danger_loc) == 2:
    # safest or most dangerous location (can be on/off/cross street) in terms of number of collisions
    print('Most {} location is {} = {} with {} collision(s)'.format(safe_danger_loc[1], safe_danger_loc[0],
                                                                    get_safe_or_danger_loc()[0],
                                                                    get_safe_or_danger_loc()[1]))
