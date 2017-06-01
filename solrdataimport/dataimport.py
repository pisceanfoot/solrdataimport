# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import urlparse
import uuid
import datetime
import cassandra
from solrdataimport.payload import Payload
from solrdataimport.dataload.fetch import FetchData
from solrdataimport.solr import SolrInterface, get_date_string
from solrdataimport.util import format_exc

logger = logging.getLogger(__name__)

class DataImport:
    def __init__(self, setting):
        self.fullDataImport = setting['fullDataImport']
        self.config_file = setting['config_file']
        self.solr_url = setting['solr_url']

        Payload.load(self.config_file)


    def exportSolr(self, name, **kwargs):
        logger.info('export sectoin "%s" to solr', name)

        section = Payload.get(name)
        if not section:
            raise Exception('section %s not found', name)

        cassData = FetchData(section)
        cassData.load(fullDataImport=self.fullDataImport, **kwargs)

        solr_core_url = urlparse.urljoin(self.solr_url, section.core_name or section.name)
        logger.debug('solr core url: %s', solr_core_url)

        solr = SolrInterface([solr_core_url])
        self.__prepareSolr(solr)

        try:
            while True:
                cassDataRows = cassData.get_rows()
                if not cassDataRows:
                    logger.debug('end export to solr')
                    break

                self.__send2Solr(solr, section, cassDataRows)
                cassData.fetch_next_page()

            self.__solrSent(solr)
            logger.debug('solr sent')
        except:
            logger.error('send solr document error %s', format_exc())

            if self.fullDataImport:
                self.__solrRollback(solr)

    def __prepareSolr(self, solr):
        logger.debug('prepare solr')
        if self.fullDataImport:
            logger.debug('full data import, delete all')

            solr.deleteAll()


    def __send2Solr(self, solr, section, cassDataRows):
        documents = []

        for row in cassDataRows:
            logger.debug('row %s', row)

            document = {}
            for item in row:
                if section.exclude and item in section.exclude:
                    continue

                item_value = row[item]
                self.__appendDocument(document, item, item_value)

                if section.solrKey:
                    array = []
                    for key in section.solrKey:
                        array.append(str(row[key.lower()]))

                    document["id"] = '#'.join(array)

            logger.debug('document %s', document)
            if document:
                documents.append(document)

        solr.add(documents)

    def __appendDocument(self, document, item, item_value):
        solr_field = None
        solr_value = None

        if item_value is None:
            return None

        value_type = type(item_value)
        if value_type is type(None):
            pass
        elif value_type is uuid.UUID:
            solr_field = item + '_s'
            solr_value = unicode(item_value)
        elif value_type in (unicode, str):
            solr_field = item + '_s'
            solr_value = item_value
        elif value_type in (int, long):
            solr_field = item + '_i'
            solr_value = unicode(item_value)
        elif value_type is float:
            solr_field = item + '_f'
            solr_value = unicode(item_value)
        elif value_type is bool:
            solr_field = item + '_b'
            solr_value = unicode(item_value)
        elif value_type is datetime.datetime:
            solr_field = item + '_dt'
            solr_value = get_date_string(item_value)
        elif value_type is cassandra.util.SortedSet:
            solr_field = item + '_ss'
            solr_value = [unicode(val) for val in item_value]

        if solr_field and solr_value:
            document[solr_field] = solr_value
            

    def __solrSent(self, solr):
        logger.debug('commit changes')
        solr.commit()

    def __solrRollback(self, solr):
        logger.debug('rollback solr change since last comit')

        solr.rollback()






    