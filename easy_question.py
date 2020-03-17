"""
Package for answering easy business questions
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
parser.add_argument('--avg_coll_rate', default=utils.HOUR,
                    help='number of collisions hourly/daily/monthly/yearly')
parser.add_argument('--num_coll_time', nargs=2, default=[utils.HOUR, '17'],
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
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.HOUR, i)))
        result = result / 24
    elif num_coll_time[0] == utils.DAY:
        for i in range(0, 7):
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.DAY, utils.get_day(i))))
        result = result / 7
    elif num_coll_time[0] == utils.MONTH:
        for i in range(1, 13):
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.MONTH, utils.get_month(i))))
        result = result / 12
    elif num_coll_time[0] == utils.YEAR:
        for i in range(utils.START_YEAR, utils.CURRENT_YEAR + 1):
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.YEAR, i)))
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
        result = int(r.get(utils.get_key(utils.TIME, city, utils.HOUR, int(num_coll_time[1]))))
    elif num_coll_time[0] == utils.DAY:
        result = int(r.get(utils.get_key(utils.TIME, city, utils.DAY, utils.get_day(int(num_coll_time[1])))))
    elif num_coll_time[0] == utils.MONTH:
        result = int(r.get(utils.get_key(utils.TIME, city, utils.MONTH, utils.get_month(int(num_coll_time[1])))))
    elif num_coll_time[0] == utils.YEAR:
        result = int(r.get(utils.get_key(utils.TIME, city, utils.HOUR, int(num_coll_time[1]))))
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
        raise ValueError('Invalid type of extremum type: ', safe_danger_time[1])
    dur = dur_func(1)
    for i in range(begin, end):
        curr_dur = dur_func(i)
        curr_val = int(r.get(utils.get_key(utils.TIME, city, duration_type, curr_dur)))
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
    if num_coll_time[0] == utils.HOUR:
        begin = 0
        end = 24
        dur, result = find_extremum_time(begin, end, num_coll_time[0], num_coll_time[1], utils.get_hour)
    elif num_coll_time[0] == utils.DAY:
        begin = 0
        end = 7
        dur, result = find_extremum_time(begin, end, num_coll_time[0], num_coll_time[1], utils.get_day)
    elif num_coll_time[0] == utils.MONTH:
        begin = 1
        end = 13
        dur, result = find_extremum_time(begin, end, num_coll_time[0], num_coll_time[1], utils.get_month)
    elif num_coll_time[0] == utils.YEAR:
        begin = utils.START_YEAR
        end = utils.CURRENT_YEAR + 1
        dur, result = find_extremum_time(begin, end, num_coll_time[0], num_coll_time[1], utils.get_year)
    else:
        raise ValueError('Invalid type of duration: ', num_coll_time[0])

    return dur, result


def get_num_coll_loc():
    """
    Function to get number of collisions in specified location (can be specified off/on/cross street)
    :return:
    """
    result = 0
    if num_coll_time[0] == utils.OFF_STREET:
        result = result + int(r.get(utils.get_key(utils.LOCATION, city, utils.OFF_STREET, int(num_coll_loc[1]))))
    elif num_coll_time[0] == utils.ON_STREET:
        result = result + int(r.get(utils.get_key(utils.LOCATION, city, utils.ON_STREET, int(num_coll_loc[1]))))
    elif num_coll_time[0] == utils.CROSS_STREET:
        result = result + int(r.get(utils.get_key(utils.LOCATION, city, utils.CROSS_STREET, int(num_coll_loc[1]))))
    else:
        raise ValueError('Invalid type of location: ', num_coll_time[0])

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
        raise ValueError('Invalid type of extremum: ', safe_danger_time[1])
    location = ''
    for key in r.keys(
            pattern=utils.COLLISION + utils.SEPARATOR + city + utils.SEPARATOR + utils.LOCATION + utils.SEPARATOR +
                    location_type + utils.SEPARATOR + '*'):
        curr_location = key.split(separator=utils.SEPARATOR)[4]
        curr_val = int(r.get(utils.get_key(utils.LOCATION, city, location_type, curr_location)))
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
    dur = 0
    if num_coll_time[0] == utils.OFF_STREET:
        result = result + int(r.get(utils.get_key(utils.LOCATION, city, utils.OFF_STREET, int(num_coll_time[1]))))
    elif num_coll_time[0] == utils.ON_STREET:
        result = result + int(r.get(utils.get_key(utils.LOCATION, city, utils.ON_STREET, utils.get_day(int(num_coll_time[1])))))
    elif num_coll_time[0] == utils.CROSS_STREET:
        result = result + int(r.get(utils.get_key(utils.LOCATION, city, utils.CROSS_STREET,
                                         utils.get_month(int(num_coll_time[1])))))
    else:
        raise ValueError('Invalid type of location: ', num_coll_time[0])

    return dur, result


if avg_coll_rate_type is not None:
    # average number of collisions in given hour/day/month
    print('Average number of collisions on {}ly basis is: {}'.format(avg_coll_rate_type, get_avg_coll_rate()))

if num_coll_time is not None and len(num_coll_time) == 2:
    # number of collisions in specified duration (can be specified hour/day/month/year)
    print('Number of collisions during {} = {} is: {}'.format(num_coll_time[0], num_coll_time[1], get_num_coll_time()))

if safe_danger_time is not None and len(safe_danger_time) == 2:
    # safest or most dangerous time (can be hour/day/month/year) in terms of number of collisions
    print('Most {} time is {} = {} with {} collisions'.format(safe_danger_time[1], safe_danger_time[0],
                                                              get_safe_or_danger_time()[0],
                                                              get_safe_or_danger_time()[1]))

if num_coll_loc is not None and len(num_coll_loc) == 2:
    # number of collisions in specified location (can be specified on/off/cross street)
    print('Number of collisions in {} = {} is: {}'.format(num_coll_loc[0], num_coll_loc[1], get_num_coll_loc()))

if safe_danger_loc is not None and len(safe_danger_loc) == 2:
    # number of collisions in given location (can be in terms of off street, on street or cross street)
    print('Most {} location is {} with respect to {} with {} collisions'.format(safe_danger_loc[1],
                                                                                get_safe_or_danger_loc()[0],
                                                                                safe_danger_loc[0],
                                                                                get_safe_or_danger_loc()[1]))
