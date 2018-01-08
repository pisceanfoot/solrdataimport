# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import copy
import json
import uuid

from solrdataimport.cass.cassClient import CassandraClient
from solrdataimport.dataload.cqlbuilder import CqlBuilder
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
        self.combineKey = combineKey
        self.exclude = exclude
        self.documentField = documentField
        self.documentId = documentId
    """
    def __init__(self, section, resultSet = None, cacheKey = None):
        self.section = section
        self.schema = None
        self.cache_fetch_end = False
        self.cacheKey = cacheKey
        
        self.main_resultSet = resultSet

    @classmethod
    def initCass(cls, cassConfig):
        config = copy.copy(cassConfig)
        cass_hosts = config.pop('hosts')
        CassandraClient.init(cass_hosts, **config)

    def loadData(self, fullDataImport=False, row=None, rowKey=None, **kwargs):
        logger.debug('load section: %s', self.section.name or self.section.table)
        logger.debug('full data import: %s', fullDataImport)
        
        search = CqlBuilder.buildCql(fullDataImport, self.section.table, 
            self.section.key, rowKey=rowKey)
        logger.debug("cql %s", search)

        params = CqlBuilder.buildParam(fullDataImport, self.section.table, 
            self.section.key, row=row, rowKey=rowKey, **kwargs)
        logger.debug("cql params %s", params)

        if self.section.cache:
            self.cacheKey = CqlBuilder.buildCacheKey(search, params)
            logger.debug("cache key %s", self.cacheKey)

            if self._InCache(self.cacheKey):
                logger.debug('loadData - hit cache: %s', self.cacheKey)
                return

        self.__loadDataFromCass(search, params)

    def _InCache(self, cacheKey):
        return Cache.hasKey(cacheKey)

    def __loadDataFromCass(self, cql, params):
        logger.debug('execute cql %s', cql)
        self.main_resultSet = CassandraClient.execute(cql, params)

    def get_rows(self):
        logger.debug('get_rows')
        if self.cache_fetch_end:
            return []

        if self.section.cache and self._InCache(self.cacheKey):
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
                        data = None
                        break

                if self.section.alias and column_name in self.section.alias:
                    column_name = self.section.alias[column_name]


                data[column_name] = column_value
            if data:
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

        logger.debug('has_more_pages => %s', self.main_resultSet.has_more_pages)
        return self.main_resultSet.has_more_pages

    def set_cache(self, data_array):
        if not self.section.cache:
            return
        if Cache.hasKey(self.cacheKey):
            return;

        logger.debug('set cache')
        Cache.set(self.cacheKey, data_array)

    def __checkCondition(self, column_name, column_value):
        logger.debug('check condition')

        check_value = None
        if isinstance(column_value, uuid.UUID):
            check_value = str(column_value)
        else:
            check_value = column_value

        return self.section.condition[column_name] == check_value
