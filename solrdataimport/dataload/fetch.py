# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
from solrdataimport.dataload.basicload import BasicLoad
from solrdataimport.map import Map

logger = logging.getLogger(__name__)

class FetchData:
    def __init__(self, section):
        self.section = section

    def load(self, fullLoad=False, **kwargs):
        self.dataload = BasicLoad(self.section)
        self.dataload.loadData(fullLoad=fullLoad, **kwargs)

    def current_rows(self):
        all_rows = self.dataload.current_rows()
        if not all_rows:
            logger.info('no row found in section %s', self.section.name)
            return []

        if self.section.nest:
            logger.debug('nest child table')
            for row in all_rows:
                for nestSection in self.section.nest:
                    self.__loadNest(nestSection, row)

        return all_rows

    def fetch_next_page(self):
        self.dataload.fetch_next_page()

    def __loadNest(self, section, row):
        section = Map(section)

        nestload = BasicLoad(section)
        nestload.loadData(row=row, rowKey=section.nestKey)

        all_rows = nestload.current_rows()
        while all_rows:
            # combine
            for nest_row in all_rows:
                row.update(nest_row)
            
            nestload.fetch_next_page()
            all_rows = nestload.current_rows()








