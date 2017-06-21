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
    host = None
    param = None

    @classmethod
    def init(cls, host, port = None, username=None, password=None):
        param = {}
        if username and password:
            param['auth_provider'] = PlainTextAuthProvider(username=username, password=password)
        if port:
            param['port'] = port

        cls.param = param
        cls.host = host

    @classmethod
    def connect(cls):
        logger.debug('cassandra connect info %s %s', cls.host, cls.param)

        cls.cluster = Cluster(cls.host, **cls.param)
        cls.session = cls.cluster.connect('system')


    @classmethod
    def execute(cls, cql, data = None):
        if not cls.session:
            cls.connect()

        logger.debug('excute cql %s : %s', cql, data)
        logger.debug(cls.session)
        if data:
            pcql = cls.session.prepare(cql)
            pcql.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            return cls.session.execute(pcql, data)
        else:
            return cls.session.execute(cql)

    @classmethod
    def wrapper(cls, column_type, param):
        if column_type == cqltypes.UUIDType and not isinstance(param, UUID):
            return UUID(param)
        if column_type == cqltypes.TimeUUIDType and not isinstance(param, UUID):
            return UUID(param)
        else:
            return param

    @classmethod
    def shutdown(cls):
        if cls.cluster:
            cls.cluster.shutdown()


