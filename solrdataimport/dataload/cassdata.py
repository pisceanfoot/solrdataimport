# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import copy
import json
import uuid
from solrdataimport.cass.cassClient import CassandraClient
from solrdataimport.cass.cassSchema import CassSchema
import solrdataimport.lib.cache as Cache

logger = logging.getLogger(__name__)


class CassandraData(object):
    """
    section:
        self.name = name
        self.cql = cql
        self.key = key
        self.nest = nest
        self.nestKey = nestKey
        self.exclude = exclude
        self.solrKey = solrKey
        self.solrId = solrId
    """
    def __init__(self, section):
        self.section = section
        self.schema = None
        self.cache_fetch_end = False
        self.cacheKey = None

    @classmethod
    def initCass(cls, cassConfig):
        config = copy.copy(cassConfig)
        cass_hosts = config.pop('hosts')
        CassandraClient.init(cass_hosts, **config)

    def loadData(self, fullDataImport=False, row=None, rowKey=None, **kwargs):
        logger.debug('load section: %s', self.section.name or self.section.table)
        logger.debug('full data import: %s', fullDataImport)
        
        self.search = self.__buildCql(fullDataImport, rowKey=rowKey)
        logger.debug("cql %s", self.search)

        params = self.__buildParam(fullDataImport, row=row, rowKey=rowKey, **kwargs)
        logger.debug("cql params %s", params)

        if self.section.cache:
            self.cacheKey = self.__buildCacheKey(self.search, params)
            logger.debug("cache key %s", self.cacheKey)

            if Cache.hasKey(self.cacheKey):
                logger.debug('loadData - hit cache: %s', self.cacheKey)
                return

        self.__loadDataFromCass(self.search, params)

    def __buildCacheKey(self, cql, params):
        return cql + '_'.join(map(str, params))

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
                column_name = resultSet.column_names[index]
                column_value = row[index]

                if self.section.condition and column_name in self.section.condition:
                    if not self.__checkCondition(column_name, column_value):
                        logger.debug('===>condition check no pass')
                        continue

                if self.section.alias and column_name in self.section.alias:
                    column_name = self.section.alias[column_name]
                data[column_name] = column_value
            data_array.append(data)

        return data_array

    def fetch_next_page(self):
        if self.section.cache and Cache.hasKey(self.cacheKey):
            logger.debug('fetch_next_page - in cache return null: %s', self.cacheKey)
            return

        logger.debug('fetch_next_page')
        self.main_resultSet.fetch_next_page()

    def has_more_pages(self):
        if self.section.cache and Cache.hasKey(self.cacheKey):
            logger.debug('has_more_pages - in cache return False: %s', self.cacheKey)
            return False

        logger.debug('has_more_pages')
        return self.main_resultSet.has_more_pages()

    def set_cache(self, data_array):
        if not self.section.cache:
            return

        logger.debug('set cache')
        Cache.set(self.cacheKey, data_array)

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
        logger.debug('fetch filed type for table "%s" field "%s"', self.section.table, field)

        schema = CassSchema.load(self.section.table)
        return schema[field.lower()]

    def __checkCondition(self, column_name, column_value):
        logger.debug('check condition')

        check_value = None
        if isinstance(column_value, uuid.UUID):
            check_value = str(column_value)
        else:
            check_value = column_value

        return self.section.condition[column_name] == check_value









