# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

from uuid import UUID

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra import cqltypes

import logging
logger = logging.getLogger(__name__)

class CassandraClient:

    cluster = None
    session = None

    @classmethod
    def init(cls, host, port = None, username=None, password=None):
        auth_provider = None

        param = {}
        if username and password:
            param['auth_provider'] = PlainTextAuthProvider(username=username, password=password)
        if port:
            param['port'] = port

        logger.debug('cassandra connect info %s %s', host, param)

        cls.cluster = Cluster(host, **param)
        

    @classmethod
    def connect(cls, keyspace):
        cls.session = cls.cluster.connect(keyspace)

    @classmethod
    def execute(cls, cql, data = None):
        if data:
            pcql = cls.session.prepare(cql)
            pcql.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            return cls.session.execute(pcql, data)
        else:
            return cls.session.execute(cql)

    @classmethod
    def wrapper(cls, column_type, param):
        if column_type == cqltypes.UUIDType:
            return UUID(param)
        if column_type == cqltypes.TimeUUIDType:
            return UUID(param)
        else:
            return param

    @classmethod
    def shutdown(cls):
        cls.cluster.shutdown()


