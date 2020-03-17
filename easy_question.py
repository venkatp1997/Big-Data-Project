"""
Package for answering easy business questions
"""

import argparse
# import collision_redis_put


parser = argparse.ArgumentParser(description="")

parser.add_argument('--avg_coll_time', default='hourly',
                    help='average number of collisions hourly/daily/monthly/yearly')
parser.add_argument('--num_coll_time', nargs=2, default= ['hour', '1700-1800'], help='average number of collisions in given hour/day/month/year')
parser.add_argument('--safe_danger_time', nargs=2, default=['hour', 'safe'], help='safest or dangerous hour/day/month/year')
parser.add_argument('--max_coll_time', default=['hour'], help='hour/day/month/year having maximums number of collisions')
parser.add_argument('--max_coll_loc', nargs=2, help='maximum number of collisions as per location type')

args = parser.parse_args()

print(args.num_coll_time)
# average number of collisions per hour/day/month
