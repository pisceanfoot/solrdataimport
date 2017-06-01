# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
from solrdataimport.dataload.fetch import FetchData
from solrdataimport.payload import Payload

logger = logging.getLogger(__name__)

class DataImport:
	def __init__(self, setting):
		self.fullLoad = setting.fullLoad
		self.config_file = setting.config_file

		Payload.load(self.config_file)


	def export(self, name, **kwargs):
		section = Payload.get(name)

		cassData = DataLoad(section)


	def __prepareSolr(self, cassData):
		if self.fullLoad：
			pass

	def __send2Solr(self, cassData):
		pass

	def __solrSent(self):
		pass




	
