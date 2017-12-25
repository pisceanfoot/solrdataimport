# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
try:
    import urlparse
    # from urllib import urlencode
except: # For Python 3
    import urllib.parse as urlparse
    # from urllib.parse import urlencode

from solrdataimport.exportclient import ExportClient
from solrdataimport.solr.solrclient import SolrInterface
from solrdataimport.solr.solrschema import build_document, build_search_key

logger = logging.getLogger(__name__)

class SolrExport(ExportClient):

    def __init__(self, solrConfig, section):
        self.solr_url = solrConfig['solr_url']
        self.fullDataImport = solrConfig.get('fullDataImport') or False
        self.solrCluster = solrConfig.get('cluster') or False
        self.commitWithIn = solrConfig.get('commitWithIn') or 1000

        if not self.solr_url.endswith('/'):
            self.solr_url = self.solr_url + '/'

        self.section = section
        self.__client = self.__getClient()

    def __getClient(self):
        solr_core_url = urlparse.urljoin(self.solr_url, self.section.core_name or self.section.name)
        logger.debug('solr core url: %s', solr_core_url)

        commitWithIn = None
        if self.solrCluster:
            commitWithIn = self.commitWithIn
        elif not self.fullDataImport:
            commitWithIn = self.commitWithIn

        return SolrInterface([solr_core_url], commitWithIn)

    def prepare(self):
        if self.fullDataImport:
            logger.debug('full data import')

            if not self.solrCluster:
                logger.debug('normal mode, reset status first')
                self.__client.rollback()

            logger.debug('send delete all command')
            self.__client.deleteAll()

    def send(self, cassDataRows):
        logger.debug('send2Solr')

        documents = []
        for row in cassDataRows:
            logger.debug('row %s', row)

            document = build_document(self.section, row)
            logger.debug('document %s', document)
            if document:
                documents.append(document)

        if documents:
            self.__client.add(documents)

    def sent(self):
        logger.debug('flush last batch')
        self.__client.flush()

        if not self.solrCluster:
            logger.debug('commit changes')
            self.__client.commit()

    def rollback(self):
        # Cluster mode not support rollback
        if self.fullDataImport and not self.solrCluster:
            logger.debug('rollback solr change since last comit')
            self.__client.rollback()

    def deleteByQuery(self, **row):
        logger.debug('delete by query')

        document = build_search_key(self.section, **row)
        logger.debug('delete document %s', document)

        self.__client.delete(document)
