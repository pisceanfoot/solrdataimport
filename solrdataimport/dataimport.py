# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import urlparse

from solrdataimport.payload import Payload
from solrdataimport.dataload.fetch import FetchData
from solrdataimport.solr import SolrInterface
from solrdataimport.solrschema import build_document, build_search_key
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

    def deleteSolr(self, name, **kwargs):
        logger.info('export sectoin "%s" to solr', name)

        section = Payload.get(name)
        if not section:
            raise Exception('section %s not found', name)

        document = build_search_key(section, **kwargs)
        logger.debug('search document %s', document)

        solr_core_url = urlparse.urljoin(self.solr_url, section.core_name or section.name)
        logger.debug('solr core url: %s', solr_core_url)

        solr = SolrInterface([solr_core_url])
        solr.delete(document)


    def __prepareSolr(self, solr):
        logger.debug('prepare solr')
        if self.fullDataImport:
            logger.debug('full data import, delete all')
            solr.rollback()
            solr.deleteAll()

    def __send2Solr(self, solr, section, cassDataRows):
        documents = []

        for row in cassDataRows:
            logger.debug('row %s', row)

            document = build_document(section, row)
            logger.debug('document %s', document)
            if document:
                documents.append(document)

        solr.add(documents)

    def __solrSent(self, solr):
        logger.debug('commit changes')
        solr.commit()

    def __solrRollback(self, solr):
        logger.debug('rollback solr change since last comit')

        solr.rollback()

