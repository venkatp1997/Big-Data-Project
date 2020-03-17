"""
Package storing common functions getting used by multiple other packages
"""

COLLISION = 'collision'
TIME = 'time'
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


def get_key(group, city_code, identifier, value):
    key = COLLISION + '_' + city_code + '_' + group + '_' + identifier + '_' + str(value)
    return str(key)


def get_month(dt_month):
    months = {1: JAN, 2: FEB, 3: MAR, 4: APR, 5: MAY, 6: JUN, 7: JUL,
              8: AUG, 9: SEP, 10: OCT, 11: NOV, 12: DEC}
    return months[dt_month]


def get_day(dt_day):
    days = {0: MONDAY, 1: TUESDAY, 2: WEDNESDAY, 3: THURSDAY, 4: FRIDAY,
            5: SATURDAY, 6: SUNDAY}
    return days[dt_day]
