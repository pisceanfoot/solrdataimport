# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement
import pylru

size = 5000          # Size of the cache. The maximum number of key/value
                    # pairs you want the cache to hold.

# TODO: TTL
__cache = pylru.lrucache(size)

def get(key):
	if key in __cache:
		return __cache[key]
	else:
		return None

def hasKey(key):
	if key in __cache:
		return True
	else:
		return False

def set(key, value):
	__cache[key] = value
