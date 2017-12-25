# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

class ExportClient(object):

    def prepare(self):
        raise NotImplementedError()

    def send(self, cassDataRows):
        raise NotImplementedError()

    def sent(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def deleteByQuery(self, row):
        raise NotImplementedError()
