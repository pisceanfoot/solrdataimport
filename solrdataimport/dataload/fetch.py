# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import logging
import copy
import json
from solrdataimport.dataload.cassdata import CassandraData

logger = logging.getLogger(__name__)

class FetchData:
    def __init__(self, section):
        self.section = section

    @classmethod
    def initCass(cls, cassConfig):
        CassandraData.initCass(cassConfig)

    def load(self, fullDataImport=False, **kwargs):
        self.dataload = CassandraData(self.section)
        self.dataload.loadData(fullDataImport=fullDataImport, **kwargs)

    def get_rows(self):
        current_rows = self.dataload.get_rows()
        if not current_rows:
            logger.debug('no row found in section %s', self.section.name)
            return []
        all_rows = current_rows

        if self.section.nest:
            logger.debug('nest child table')
            for nestSection in self.section.nest:
                
                rows_after_combine = []
                for row in all_rows:
                    new_rows = self.__loadNest(nestSection, row)
                    if new_rows:
                        rows_after_combine = rows_after_combine + new_rows
                all_rows = copy.copy(rows_after_combine)


        if self.section.combine:
            logger.debug('combine child table as a new field')

            for combineSection in self.section.combine:
                for row in all_rows:
                    self.__setCombine(combineSection, row)
        
        return all_rows

    def fetch_next_page(self):
        self.dataload.fetch_next_page()

    def has_more_pages(self):
        self.dataload.has_more_pages()

    def __loadNest(self, section, row):
        nestload = CassandraData(section)
        nestload.loadData(row=row, rowKey=section.nestKey)

        current_rows = nestload.get_rows()
        if not current_rows:
            logger.debug('empty nest row %s', section.table)
            return None

        copy_all_rows = []
        while current_rows:
            copy_all_rows = copy_all_rows + current_rows

            if nestload.has_more_pages():
                nestload.fetch_next_page()
                current_rows = nestload.get_rows()
            else:
                break

        nestload.set_cache(copy_all_rows)

        combine_array = []
        for nest_item in copy_all_rows:
            new_item = nest_item.copy()
            new_item.update(row)

            combine_array.append(new_item)

        return combine_array

    def __setCombine(self, section, row):
        combineload = CassandraData(section)
        combineload.loadData(row=row, rowKey=section.combineKey)

        current_rows = combineload.get_rows()
        if not current_rows:
            logger.debug('empty nest row %s', section.table)
            return

        copy_all_rows = []
        while current_rows:
            copy_all_rows = copy_all_rows + current_rows

            if combineload.has_more_pages():
                combineload.fetch_next_page()
                current_rows = combineload.get_rows()
            else:
                break

        combineload.set_cache(copy_all_rows)

        if copy_all_rows:
            if section.field_map_one2one:
                copy_all_rows = copy_all_rows[0]

            if section.field_type == "string":
                row[section.field_name] = json.dumps(copy_all_rows, ensure_ascii=False)
            else:
                row[section.field_name] = copy_all_rows
                    







