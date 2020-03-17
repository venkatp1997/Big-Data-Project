"""
Package for answering easy business questions
"""

import argparse
import redis
import collision_redis_put
import utils

# constants used


# Launch connector to redis.
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# define and parse the arguments passed
parser = argparse.ArgumentParser(description='Utility to answer easy business questions')

parser.add_argument('--city', default='NYC', help='city under consideration')
parser.add_argument('--avg_coll_time', default=utils.HOUR,
                    help='average number of collisions hourly/daily/monthly/yearly')
parser.add_argument('--num_coll_time', nargs=2, default=[utils.HOUR, '17-18'],
                    help='average number of collisions in given hour/day/month/year')
parser.add_argument('--safe_danger_time', nargs=2, default=[utils.HOUR, 'safe'],
                    help='safest or dangerous hour/day/month/year')
parser.add_argument('--max_coll_time', default=utils.HOUR,
                    help='hour/day/month/year having maximums number of collisions')
parser.add_argument('--max_coll_loc', default=utils.ON_STREET, help='maximum number of collisions as per location type')
args = parser.parse_args()

result = None
city = args.city
# average number of collisions per hour/day/month
avg_col_rate_type = args.avg_coll_time
if avg_col_rate_type is not None:
    if avg_col_rate_type == utils.HOUR:
        result = 0
        for i in range(0, 24):
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.HOUR, i)))
        result = result/24
    elif avg_col_rate_type == utils.DAY:
        result = 0
        for i in range(0, 7):
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.DAY, utils.get_day(i))))
        result = result/7
    elif avg_col_rate_type == utils.MONTH:
        result = 0
        for i in range(1, 13):
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.MONTH, utils.get_month(i))))
        result = result / 12
    elif avg_col_rate_type == utils.YEAR:
        result = 0
        for i in range(0, 7):
            result = result + int(r.get(utils.get_key(utils.TIME, city, utils.YEAR, utils.get_day(i))))
        result = result / 7
    else:
        raise ValueError('Invalid type of rate: ', avg_col_rate_type)

    print('Average number of collisions on {}ly basis is: {}'.format(avg_col_rate_type, result))

