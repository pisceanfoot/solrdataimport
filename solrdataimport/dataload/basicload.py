# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import json
import uuid
from solrdataimport.dataload.cassClient import CassandraClient
from solrdataimport.dataload.cassSchema import CassSchema
import solrdataimport.dataload.cache as Cache

logger = logging.getLogger(__name__)


class BasicLoad(object):
    """
    section:
        self.name = name
        self.cql = cql
        self.key = key
        self.nest = nest
        self.nestKey = nestKey
        self.exclude = exclude
        self.solrKey = solrKey
    """
    def __init__(self, section):
        self.section = section
        self.schema = None
        self.cache_fetch_end = False
        self.cacheKey = None

    def loadData(self, fullDataImport=False, row=None, rowKey=None, **kwargs):
        logger.debug('load section: %s', self.section.name or self.section.table)
        logger.info('full data import: %s', fullDataImport)
        
        self.search = self.__buildCql(fullDataImport, rowKey=rowKey)
        logger.debug("cql %s", self.search)

        params = self.__buildParam(fullDataImport, row=row, rowKey=rowKey, **kwargs)
        logger.debug("cql params %s", params)

        if self.section.cache:
            self.cacheKey = self.__buildCacheKey(self.search, params)

            if Cache.hasKey(self.cacheKey):
                logger.info('loadData - hit cache: %s', self.cacheKey)
                return

        self.__loadDataFromCass(self.search, params)

    def __buildCacheKey(self, cql, params):
        array = []

        if params:
            for x in params:
                if isinstance(x, uuid.UUID):
                    array.append(str(x))
                else:
                    array.append(x)

        return cql + '_'.join(array)

    def __loadDataFromCass(self, cql, params):
        logger.debug('execute cql %s', cql)
        self.main_resultSet = CassandraClient.execute(cql, params)

    def get_rows(self):
        logger.debug('get_rows')
        if self.cache_fetch_end:
            return []

        if self.section.cache and Cache.hasKey(self.cacheKey):
            logger.debug('current_rows - hit cache: %s', self.cacheKey)
            self.cache_fetch_end = True
            return Cache.get(self.cacheKey)

        resultSet = self.main_resultSet
        current_rows = resultSet.current_rows

        if not current_rows:
            return []

        logger.debug('rows count %s', len(current_rows))

        column_length = len(resultSet.column_names)
        data_array = []
        for row in current_rows:

            data = {}
            for index in range(column_length):
                data[resultSet.column_names[index]] = row[index]
            data_array.append(data)

        self.__set_cache(data_array)
        return data_array

    def __set_cache(self, data_array):
        if not self.section.cache:
            return

        array = Cache.get(self.cacheKey)
        if array:
            data_array = array + data_array

        logger.debug('set cache')
        Cache.set(self.cacheKey, data_array)

    def fetch_next_page(self):
        if self.section.cache and Cache.hasKey(self.cacheKey):
            logger.debug('fetch_next_page - in cache return null: %s', self.cacheKey)
            return

        logger.debug('fetch_next_page')
        self.main_resultSet.fetch_next_page()

    def __buildCql(self, fullDataImport, rowKey=None):
        cql = 'select * from {0}'.format(self.section.table)
        self.cql = cql

        appendKey = []
        if not fullDataImport and self.section.key:
            appendKey = self.section.key
        if rowKey:
            for key in rowKey:
                appendKey.append(key)

        if appendKey:
            key = ' = ? and '.join(appendKey)
            cql = cql + ' where ' + key + ' = ?;'

        return cql

    def __buildParam(self, fullDataImport, row=None, rowKey=None, **kwargs):
        if fullDataImport:
            return None

        params = []
        if self.section.key:
            for x in self.section.key:
                if x not in kwargs:
                    raise Exception('key %s not found in param', x)

                column_type = self.__fetchFieldType(x)
                params.append(CassandraClient.wrapper(column_type, kwargs.pop(x)))
        
        if row and rowKey:
            for key in rowKey:
                fetchKey = rowKey[key].lower()

                column_type = self.__fetchFieldType(key)
                params.append(CassandraClient.wrapper(column_type, row[fetchKey]))

        return params

    def __fetchFieldType(self, field):
        schema = CassSchema.load(self.section.table)
        return schema[field.lower()]

