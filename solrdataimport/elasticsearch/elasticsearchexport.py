# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import datetime
try:
    import urlparse
    # from urllib import urlencode
except: # For Python 3
    import urllib.parse as urlparse
    # from urllib.parse import urlencode

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from solrdataimport.exportclient import ExportClient
from solrdataimport.elasticsearch.elasticsearchschema import build_document, build_search_key, build_document_key

logger = logging.getLogger(__name__)

NEW_INDEX_NAME_SEPARATOR = '^666^'

class ElasticSearchExport(ExportClient):

    def __init__(self, esConfig, section):
        self.elastic_endpoints = esConfig['elastic_endpoints']

        self.fullDataImport = esConfig.get('fullDataImport') or False
        self.rebuild_all = esConfig.get('rebuild_all') or False
        self.index_template = esConfig.get('index_template')
        
        self.section = section
        self.index_name = self.section.index_name
        self.type_name = self.section.type_name

        self.__client = self.__getClient()

    def __getClient(self):
        es = Elasticsearch(self.elastic_endpoints)
        return es

    def prepare(self):
        if self.fullDataImport and self.rebuild_all:
            logger.info('full data import and rebuild all type in index %s', 
                self.section.index_name)

            # rename a new index
            self.index_name = self.section.index_name + NEW_INDEX_NAME_SEPARATOR +\
                datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            logger.debug('new index name %s', self.index_name)

            current_index_setting = None
            if self.index_template:
                index_template_name = self.index_template.get(self.index_name)
                tmp = self.__client.indices.get_template(index_template_name, ignore=[404])
                
                if tmp:
                    current_index_setting = tmp

            self.__client.indices.create(self.index_name, body=current_index_setting)
            
    def send(self, cassDataRows):
        logger.debug('send 2 es')

        documents = []
        for row in cassDataRows:
            logger.debug('row %s', row)

            document = build_document(self.section, row)
            if document:
                documentId = build_document_key(self.section, row)
                logger.debug('document ==> %s', document)
                logger.debug('document id %s', documentId)

                documents.append({
                    "id": documentId,
                    "body": document
                })

        if not documents:
            return

        if len(documents) > 1:
            logger.info('bulk update documents %s', len(documents))
            helpers.bulk(self.__client, self.__bulkSet(documents))
        else:
            logger.info('send update index command')
            self.__client.index(self.index_name, 
                body = documents[0]['body'], 
                doc_type = self.type_name,
                id = documents[0]['id'])

    def __bulkSet(self, documents):
        for doc in documents:
            yield {
                "_index": self.index_name,
                "_type": self.type_name,
                "_id": doc['id'],
                "_source": doc['body']
            }

    def sent(self):
        logger.info('document sent')

        if not self.fullDataImport or not self.rebuild_all:
            return

        logger.info('create alia for new index')
        print(self.section.index_name + NEW_INDEX_NAME_SEPARATOR +'*')
        alias = self.__client.indices.get_alias(index=self.section.index_name + NEW_INDEX_NAME_SEPARATOR +'*', ignore=[404])
        alias = self.__parseAlias(alias)
        if not alias:
            alias = self.__client.indices.get_alias(index=self.section.index_name, ignore=[404])
            alias = self.__parseAlias(alias)

        pre_index_name = None
        all_aliases = None
        if alias:
            pre_index_name = alias['index']
            all_aliases = alias['alias']
        else:
            all_aliases = {
                self.section.index_name: {}
            }

        if pre_index_name:
            logger.info('delete alias on index "%s"', pre_index_name)
            self.__client.indices.delete_alias(index = pre_index_name, name = '_all', ignore = [404])
        else:
            logger.info('alia name == index name, delete old index first: %s', self.section.index_name)
            self.__client.indices.delete(self.section.index_name, ignore=[404])

        for name in all_aliases:
            setting = all_aliases[name]
            logger.info('create "%s" alias on index "%s"', name, self.index_name)
            self.__client.indices.put_alias(index = self.index_name, name = name, body = setting)

        if pre_index_name:
            logger.info('delete old index %s', pre_index_name)
            self.__client.indices.delete(pre_index_name, ignore=[404])

        logger.info('sent operation done')

    def __parseAlias(self, alias):
        if not alias:
            return None

        alias_result = {}
        for name in alias:
            value = alias[name]
            current = value.get('aliases')
            if current:
                alias_result['index'] = key
                alias_result['alias'] = current

            break

        return alias_result

    def rollback(self):
        if self.fullDataImport and self.rebuild_all:
            if self.index_name == self.section.index_name:
                return

            logger.info('rollback, delete current index %s', self.index_name)
            self.__client.indices.delete(self.index_name, ignore=[404,400])

    def deleteByQuery(self, row):
        logger.info('delete by query in section %s', self.section.name)

        document = build_search_key(self.section, **row)
        logger.debug('delete document %s', document)

        array = []
        for x in document:
            array.append(x + ":" + doc[x])

        del_command = " AND ".join(map(str, array))
        logger.debug('delete command %s', del_command)

        self.__client.delete_by_query(index = self.section.index_name, 
            doc_type = self.section.type_name,
            q = del_command)
