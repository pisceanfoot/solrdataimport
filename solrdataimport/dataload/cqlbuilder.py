# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging

from solrdataimport.cass.cassClient import CassandraClient
from solrdataimport.cass.cassSchema import CassSchema

logger = logging.getLogger(__name__)

class CqlBuilder(object):

    @classmethod
    def buildCacheKey(cls, cql, params):
        return cql + '_'.join(map(str, params))

    @classmethod
    def buildCql(cls, fullDataImport, table, table_key, rowKey=None):
        cql = 'select * from {0}'.format(table)

        appendKey = []
        if not fullDataImport and table_key:
            appendKey = table_key
        if rowKey:
            for key in rowKey:
                appendKey.append(key)

        if appendKey:
            key = ' = ? and '.join(appendKey)
            cql = cql + ' where ' + key + ' = ?;'

        return cql

    @classmethod
    def buildParam(cls, fullDataImport, table, table_key, row=None, rowKey=None, **kwargs):
        if fullDataImport:
            return None

        params = []
        if table_key:
            for x in table_key:
                if x not in kwargs:
                    raise Exception('key %s not found in param', x)

                column_type = cls.__fetchFieldType(table, x)
                params.append(CassandraClient.wrapper(column_type, kwargs.pop(x)))
        
        if row and rowKey:
            for key in rowKey:
                fetchKey = rowKey[key].lower()

                column_type = cls.__fetchFieldType(table, key)
                params.append(CassandraClient.wrapper(column_type, row[fetchKey]))

        return params

    @classmethod
    def __fetchFieldType(cls, table, field):
        logger.debug('fetch filed type for table "%s" field "%s"', table, field)

        schema = CassSchema.load(table)
        field_name_lower = field.lower()

        if field_name_lower in schema:
            return schema[field_name_lower]
        else:
            logger.error('field "%s" not in table "%s"', field, table)
            raise Exception('field "%s" not in table "%s"', field, table)

