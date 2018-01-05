# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement
import logging

from solrdataimport.lib.util import format_exc
from solrdataimport.lib.map import Map

from solrdataimport.payload import Payload
from solrdataimport.dataload.fetch import FetchData
from solrdataimport.solr.solrexport import SolrExport
from solrdataimport.elasticsearch.elasticsearchexport import ElasticSearchExport

logger = logging.getLogger(__name__)

class DataImport:
    def __init__(self, setting):
        setting = Map(setting)

        self.fullDataImport = setting.fullDataImport or False
        self.config = {
            "fullDataImport": self.fullDataImport,

            # solr setting
            "solr_url": setting.solr_url,
            "cluster": setting.solr_cluster or False,
            "commitWithin": setting.commitWithin or 1000,

            # es setting
            "elastic_endpoints": setting.elastic_endpoints,
            "index_template": setting.index_template,
            "rebuild_all": setting.rebuild_all
        }

        # cass setting
        FetchData.initCass(setting.cassandra)
        Payload.load(setting.config_file)

    def __getExportHandler(self, section):
        if self.config.get('solr_url'):
            logger.info('-_-solr export-_-')
            return SolrExport(self.config, section)
        elif self.config.get('elastic_endpoints'):
            logger.info('-_-es export-_-')
            return ElasticSearchExport(self.config, section)
        else:
            raise Exception('config not correct')

    def export(self, name, **kwargs):
        logger.info('export section "%s"', name)

        section = Payload.get(name)
        if not section:
            raise Exception('section %s not found', name)

        if self.fullDataImport and \
            self.config.get('rebuild_all', False) == True:
            logger.info('rebuild_all')

            section_list = Payload.get_all_index(section.index_name)
        else:
            section_list = [section]

        self.__export(section_list, **kwargs)
        

    def __export(self, section_list, **kwargs):
        if not section_list:
            raise Exception('section %s not found', name)

        logger.info('export section count (%s)', len(section_list))
        for section in section_list:
            self.__exportSection(section, **kwargs)

    def __exportSection(self, section, **kwargs):
        logger.info('export section %s', section.name)

        cassData = FetchData(section)
        cassData.load(fullDataImport=self.fullDataImport, **kwargs)

        exportHandler = self.__getExportHandler(section)
        try:
            exportHandler.prepare()

            total = 0
            while True:
                cassDataRows = cassData.get_rows()
                if not cassDataRows:
                    logger.debug('get empty row, export end')
                    break

                currentBatch = len(cassDataRows)
                total = total + currentBatch
                logger.info('current data loads (%s), total (%s)', currentBatch, total)
                
                exportHandler.send(cassDataRows)
                logger.info('export data sent, will fetch next batch')

                if cassData.has_more_pages():
                    logger.info('has more pages True ===> True')
                    cassData.fetch_next_page()
                else:
                    logger.info('no more page == done')
                    break

            exportHandler.sent()

            logger.info('export data sent completely')
        except:
            errorInfo = format_exc()
            logger.error('send document in section "%s" error %s', section, errorInfo)
            exportHandler.rollback()
            raise Exception('send document in section "%s" error %s', section, errorInfo)

    def delete(self, name, **kwargs):
        logger.info('export section "%s"', name)

        section = Payload.get(name)
        if not section:
            raise Exception('section %s not found', name)

        logger.debug('section %s', section)

        exportHandler = self.__getExportHandler(section)
        exportHandler.deleteByQuery(**kwargs)

