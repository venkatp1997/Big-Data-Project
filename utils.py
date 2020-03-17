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

SEPARATOR = '#'


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
