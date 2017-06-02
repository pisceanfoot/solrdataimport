# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement
import pylru
import datetime

size = 5000          # Size of the cache. The maximum number of key/value
                    # pairs you want the cache to hold.

# TODO: TTL
__cache = pylru.lrucache(size)

def get(key):
	if key in __cache:
		cache_item = __cache[key]

		now = datetime.datetime.now()
		enddate = cache_item['enddate']

		if (enddate - now).total_seconds() > 0:
			return cache_item['value']
		else:
			del __cache[key]
			return None
	else:
		return None

def hasKey(key):
	if key in __cache:
		return True
	else:
		return False

def set(key, value, ttl=120):
	now = datetime.datetime.now()
	enddate = now + datetime.timedelta(seconds=ttl)
	__cache[key] = {
		"value": value,
		"enddate": enddate
	}
