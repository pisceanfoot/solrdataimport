# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
from solrdataimport.cassClient import CassandraClient
import solrdataimport.cache as Cache

logger = logging.getLogger(__name__)

class CassSchema:

	@classmethod
	def load(cls, table):
		logger.debug('load schema for table %s', table)

		schema = Cache.get(table)
		if schema:
			logger.debug('load schema for cache %s', schema)
			return schema

		cql = 'select * from {0} limit 1;'.format(table)
		logger.debug('cql %s', cql)

		resultSet = CassandraClient.execute(cql)

		index = 0
		schema = {}

		for column in resultSet.column_names:
			schema[column] = resultSet.column_types[index]
			index += 1

		Cache.set(table, schema)

		return schema


