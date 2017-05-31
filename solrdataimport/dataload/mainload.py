# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
from solrdataimport.dataload.baseload import BaseLoad

logger = logging.getLogger(__name__)

class MainLoad(BaseLoad):
	def __init__(self):
		super(MainLoad).__init__(self)