# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement
import pylru

size = 1000          # Size of the cache. The maximum number of key/value
                    # pairs you want the cache to hold.

__cache = pylru.lrucache(size)

def get(key):
	if key in __cache:
		return __cache[key]
	else:
		return None

def set(key, value):
	__cache[key] = value
