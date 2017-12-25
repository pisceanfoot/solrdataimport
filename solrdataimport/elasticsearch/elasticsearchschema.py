import logging
import uuid
import datetime

import cassandra
from cassandra import cqltypes

logger = logging.getLogger(__name__)

def build_document_key(section, row):
    if not section.documentId:
        return None

    array = []
    for key in section.documentId:
        array.append(str(row[key]))

    documentId = '#'.join(array)
    return documentId

def build_document(section, row):
    document = {}
    for item in row:
        if section.exclude and item in section.exclude:
            continue
        if section.documentField and not item in section.documentField:
            continue

        item_value = row[item]
        if item_value is None:
             continue

        value_type = type(item_value)
        es_value = None

        if value_type is type(None):
            continue
        elif value_type is uuid.UUID:
            es_value = unicode(item_value)
        elif value_type in (unicode, str):
            es_value = item_value
        elif value_type in (int, long):
            es_value = unicode(item_value)
        elif value_type is float:
            es_value = unicode(item_value)
        elif value_type is bool:
            es_value = unicode(item_value)
        elif value_type is cassandra.util.SortedSet:
            es_value = [unicode(val) for val in item_value]
        else:
            es_value = item_value

        if es_value:
            document[item] = es_value

    return document

def build_search_key(section, **kwargs):
    row = {}

    for x in section.key:
        if x not in kwargs:
            raise Exception('key %s not found in param', x)
        row[x] = kwargs.pop(x)

    search = {}
    for name in row:
        value = row[name]
        if value is None:
            continue

        search[name.lower()] = value

    return search
