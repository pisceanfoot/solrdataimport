# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import solrdataimport.lib.cache as Cache
from solrdataimport.cass.cassClient import CassandraClient
from solrdataimport.dataload.cqlbuilder import CqlBuilder
from solrdataimport.dataload.cassdata import CassandraData

logger = logging.getLogger(__name__)

class CassandraDataBatchLoad(object):

    @classmethod
    def batchResult(cls, section, rows, rowKey):
        logger.debug('load section: %s', section.name or section.table)
        
        if len(rows) == 1:
            logger.debug('one row')
            cassData = CassandraData(section)
            cassData.loadData(fullDataImport = False, row = rows[0], rowKey = rowKey)

            return [cassData]
        else:
            logger.info('more rows, batch mode')
            return cls.__batchLoad(section, rows, rowKey)

    @classmethod
    def __batchLoad(cls, section, rows, rowKey):
        search = CqlBuilder.buildCql(False, section.table, 
            section.key, rowKey=rowKey)
        logger.debug("cql %s", search)

        statements_and_params = []
        for row in rows:

            params = CqlBuilder.buildParam(False, section.table, 
            section.key, row = row, rowKey = rowKey)
            logger.debug("cql params %s", params)

            cacheKey = None
            if section.cache:
                cacheKey = CqlBuilder.buildCacheKey(search, params)
                logger.debug("cache key %s", cacheKey)

                if Cache.hasKey(cacheKey):
                    logger.debug('loadData - hit cache: %s', cacheKey)
                    statements_and_params.append({"cached": true, 
                        "cacheKey": cacheKey})
                    continue
            
            query = {
                "cql": search,
                "params": params,
                "cacheKey": cacheKey
            }
            statements_and_params.append(query)

        statements_need_query = []
        for query in statements_and_params:
            if query.has_key('cached'):
                continue

            statements_need_query.append(query)

        logger.debug('batch load ====>>, total %s', len(statements_need_query))
        resultSetArray = CassandraClient.execute_concurrent(statements_need_query)
        logger.debug('batch load done')

        index = 0
        PreloadDataArray = []
        for query in statements_and_params:
            preload = None
            if query.has_key('cached'):
                preload = CassandraData(section, None, query['cacheKey'])
            else:
                success, resultSet = resultSetArray[index]
                if success:
                    preload = CassandraData(section, resultSet, query['cacheKey'])
                index = index + 1

            if preload:
                PreloadDataArray.append(preload)

        return PreloadDataArray
