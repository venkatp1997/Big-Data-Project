"""
Package storing common functions getting used by multiple other packages
"""


def get_key(group, identifier, value):
    key = "collision"+'_'+city_code+'_'+group+'_'+identifier+'_'+str(value)
    return str(key)
