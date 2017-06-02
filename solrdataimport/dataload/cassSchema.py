# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
from solrdataimport.dataload.cassClient import CassandraClient
import solrdataimport.dataload.cache as Cache

logger = logging.getLogger(__name__)

class CassSchema:

	@classmethod
	def load(cls, table):
		logger.debug('load schema for table %s', table)

		schema = Cache.get('SCHEMA_' + table)
		if schema:
			logger.debug('load schema from cache %s', schema)
			return schema

		cql = 'select * from {0} limit 1;'.format(table)
		logger.debug('schema cql %s', cql)

		resultSet = CassandraClient.execute(cql)

		index = 0
		schema = {}

		for column in resultSet.column_names:
			schema[column] = resultSet.column_types[index]
			index += 1

		Cache.set('SCHEMA_' + table, schema, 3600)

		return schema


