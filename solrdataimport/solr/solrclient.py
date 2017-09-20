# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import calendar
from datetime import datetime
import json
import logging
import time
import types
import urllib
import urllib2
import urlparse
import pytz

SOLR_ADD_BATCH = 200 # Number of documents to send in batch when adding
logger = logging.getLogger(__name__)

def get_date_string(date):
    try:
        if date:
            return date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except Exception as ex:
        return None

def to_solr_date(date):
    """
    Solr demands ISO8601 time format in UTC timezone. This converts TZ and formats to compatible format.
    """
    utc_date = date.astimezone(pytz.utc)
    return utc_date.strftime("%Y-%m-%dT%H:%M:%SZ")

class SolrInterface(object):
    def __init__(self, endpoints):
        self._add_batch = []
        if not endpoints:
            logger.warning("Faulty Solr configuration, SOLR will not be available!")
        self.endpoints = endpoints


    def _send_solr_command(self, core_url, json_command):
        """
        Sends JSON string to Solr instance
        """

        # Check document language and dispatch to correct core
        url = urlparse.urljoin(core_url + '/', "update?overwrite=true&wt=json&commitWithin=1000")
        try:
            request = urllib2.Request(url, json_command, {'Content-Type':'application/json'})
            response = urllib2.urlopen(request).read()
        except urllib2.HTTPError as e:
            logger.error("Failed to send update to Solr endpoint [%s]: %s", core_url, e, exc_info=True)
            return False
        return True

    def add(self, documents):
        """
        Adds documents to Solr index
        documents - Single item or list of items to add
        """

        if isinstance(documents, types.ListType):
            self._add_batch.extend(documents)
        else:
            self._add_batch.append(documents)

        if len(self._add_batch) > SOLR_ADD_BATCH:
            self._addFlushBatch()

    def flush(self):
        self._addFlushBatch()

    def _addFlushBatch(self):
        """
        Sends all waiting documents to Solr
        """
        if len(self._add_batch) > 0:
            for core in self.endpoints:
                document_jsons = [json.dumps(data) for data in self._add_batch]
                command_json = "[" + ",".join(document_jsons) + "]"

                self._send_solr_command(core, command_json)

            self._add_batch = []

    def deleteAll(self):
        """
        Deletes whole Solr index. Use with care.
        """
        for core in self.endpoints:
            self._send_solr_command(core, "{\"delete\": { \"query\" : \"*:*\"}}")

    def delete(self, doc):
        """
        Deletes document with ID on all Solr cores
        """

        array = []
        for x in doc:
            array.append(x + ":" + doc[x])

        del_command = "{\"delete\" : { \"query\": \"" + " AND ".join(map(str, array)) + "\"}}"
        logger.debug('delete command %s', del_command)

        for core in self.endpoints:
            self._send_solr_command(core, del_command)

    def rollback(self):
        """
        rollback document changes
        """
        for core in self.endpoints:
            self._send_solr_command(core, "{\"rollback\":{} }")

    def commit(self):
        """
        Flushes all pending changes and commits Solr changes
        """
        self._addFlushBatch()
        for core in self.endpoints:
            self._send_solr_command(core, "{ \"commit\":{} }")

    def optimize(self):
        for core in self.endpoints:
            self._send_solr_command(core, "{ \"optimize\": {} }")


