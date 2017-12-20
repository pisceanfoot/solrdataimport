# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement
import logging

from solrdataimport.lib.util import format_exc
from solrdataimport.lib.map import Map

from solrdataimport.payload import Payload
from solrdataimport.dataload.fetch import FetchData
from solrdataimport.solr.solrexport import SolrExport

logger = logging.getLogger(__name__)

class DataImport:
    def __init__(self, setting):
        setting = Map(setting)

        self.fullDataImport = setting.fullDataImport or False
        # solr setting
        self.solrConfig = {
            "solr_url": setting.solr_url,
            "fullDataImport": self.fullDataImport,
            "cluster": setting.solr_cluster or False,
            "commitWithin": setting.commitWithin or 1000
        }

        # cass setting
        FetchData.initCass(setting.cassandra)

        Payload.load(setting.config_file)

    def exportSolr(self, name, **kwargs):
        logger.info('export sectoin "%s" to solr', name)

        section = Payload.get(name)
        if not section:
            raise Exception('section %s not found', name)

        cassData = FetchData(section)
        cassData.load(fullDataImport=self.fullDataImport, **kwargs)

        solrExport = SolrExport(self.solrConfig, section)
        solrExport.prepareSolr()

        try:
            while True:
                cassDataRows = cassData.get_rows()
                if not cassDataRows:
                    logger.debug('end export to solr')
                    break

                solrExport.send2Solr(cassDataRows)
                cassData.fetch_next_page()

            solrExport.solrSent()
            logger.debug('solr sent')
        except:
            logger.error('send solr document error %s', format_exc())
            solrExport.solrRollback()

    def deleteSolr(self, name, **kwargs):
        logger.info('export sectoin "%s" to solr', name)

        section = Payload.get(name)
        if not section:
            raise Exception('section %s not found', name)

        solrExport = SolrExport(self.solrConfig, section)
        solrExport.deleteByQuery(kwargs)


