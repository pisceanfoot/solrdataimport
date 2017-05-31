# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
from solrdataimport.cassClient import CassandraClient
from solrdataimport.cassSchema import CassSchema

logger = logging.getLogger(__name__)


class BaseLoad:
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

    def load(self, fullLoad=False, **kwargs):
        logger.debug('load section: %s', self.section.name)
        logger.info('full load: %s', fullLoad)

        cql = self.__buildCql(fullLoad)
        logger.debug("cql %s", cql)

        params = self.__buildParam(fullLoad, **kwargs)
        logger.debug("params %s", params)

        self.__loadDataFromCass(cql, params)

    def __loadDataFromCass(self, cql, params):
        self.main_resultSet = CassandraClient.execute(cql, params)


    def current_rows(self):
        resultSet = self.main_resultSet
        current_rows = resultSet.current_rows

        if not current_rows:
            return None

        column_length = len(resultSet.column_names)
        data_array = []
        for row in current_rows:

            data = {}
            for index in range(column_length):
                data[resultSet.column_names[index]] = row[index]
            data_array.append(data)

        if self.section.nest:
            self.__nest(data_array)

    def __nest(self, data_array):
        pass

    def has_more_pages(self):
        pass

    def fetch_next_page(self):
        pass


    def __buildCql(self, fullLoad):
        cql = 'select * from {0}'.format(self.section.table)
        self.cql = cql

        if not fullLoad:
            key = ' = ? and '.join(self.section.key)
            cql = cql + ' where ' + key + ' = ?;'
        
        return cql

    def __buildParam(self, fullLoad, **kwargs):
        if fullLoad:
            return None

        params = []
        print(kwargs)

        for x in self.section.key:
            if x not in kwargs:
                raise Exception('key %s not found in param', x)

            column_type = self.__fetchFieldType(x)
            params.append(CassandraClient.wrapper(column_type, kwargs.pop(x)))

        return params

    def __fetchFieldType(self, field):
        schema = CassSchema.load(self.section.table)
        return schema[field.lower()]

if __name__ == '__main__':
    from payload import Payload

    import logging.config
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)-s: %(message)s')

    




