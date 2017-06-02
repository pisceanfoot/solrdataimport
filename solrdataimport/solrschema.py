import logging
import uuid
import datetime
import cassandra
from solrdataimport.dataload.cassSchema import CassSchema
from cassandra import cqltypes
from solrdataimport.solr import get_date_string

logger = logging.getLogger(__name__)

def build_document(section, row):
    document = {}
    for item in row:
        if section.exclude and item in section.exclude:
            continue
        if section.solrKey and not item in section.solrKey:
            continue

        item_value = row[item]

        __appendDocument(document, item, item_value)

        if section.solrId:
            array = []
            for key in section.solrId:
                array.append(str(row[key]))

            document["id"] = '#'.join(array)
    return document

def __appendDocument(document, item, item_value):
        solr_field = None
        solr_value = None

        if item_value is None:
            return None

        value_type = type(item_value)
        if value_type is type(None):
            pass
        elif value_type is uuid.UUID:
            solr_field = item + '_s'
            solr_value = unicode(item_value)
        elif value_type in (unicode, str):
            solr_field = item + '_s'
            solr_value = item_value
        elif value_type in (int, long):
            solr_field = item + '_i'
            solr_value = unicode(item_value)
        elif value_type is float:
            solr_field = item + '_f'
            solr_value = unicode(item_value)
        elif value_type is bool:
            solr_field = item + '_b'
            solr_value = unicode(item_value)
        elif value_type is datetime.datetime:
            solr_field = item + '_dt'
            solr_value = get_date_string(item_value)
        elif value_type is cassandra.util.SortedSet:
            solr_field = item + '_ss'
            solr_value = [unicode(val) for val in item_value]

        if solr_field and solr_value:
            document[solr_field] = solr_value    

def build_search_key(section, **kwargs):
    row = {}

    for x in section.key:
        if x not in kwargs:
            raise Exception('key %s not found in param', x)
        row[x] = kwargs.pop(x)

    schema = CassSchema.load(section.table)

    search = {}
    for name in row:
        value = row[name]
        if value is None:
            continue

        new_name = None
        lower_name = name.lower()

        fieldType = schema[lower_name]
        if fieldType == cqltypes.BytesType:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.DecimalType:
            new_name = lower_name + '_f'
        elif fieldType == cqltypes.UUIDType:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.BooleanType:
            new_name = lower_name + '_b'
        elif fieldType == cqltypes.ByteType:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.FloatType:
            new_name = lower_name + '_f'
        elif fieldType == cqltypes.AsciiType:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.DoubleType:
            new_name = lower_name + '_f'
        elif fieldType == cqltypes.LongType:
            new_name = lower_name + '_i'
        elif fieldType == cqltypes.Int32Type:
            new_name = lower_name + '_i'
        elif fieldType == cqltypes.IntegerType:
            new_name = lower_name + '_i'
        elif fieldType == cqltypes.InetAddressType:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.CounterColumnType:
            new_name = lower_name + '_i'
        elif fieldType == cqltypes.TimestampType:
            new_name = lower_name + '_dt'
        elif fieldType == cqltypes.TimeUUIDType:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.SimpleDateType:
            new_name = lower_name + '_dt'
        elif fieldType == cqltypes.ShortType:
            new_name = lower_name + '_dt'
        elif fieldType == cqltypes.TimeType:
            new_name = lower_name + '_dt'
        elif fieldType == cqltypes.DurationType:
            new_name = lower_name + '_dt'
        elif fieldType == cqltypes.UTF8Type:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.VarcharType:
            new_name = lower_name + '_s'
        elif fieldType == cqltypes.ListType:
            new_name = lower_name + '_ss'
        elif fieldType == cqltypes.SetType:
            new_name = lower_name + '_ss'
        elif fieldType == cqltypes.MapType:
            new_name = lower_name + '_ss'
        elif fieldType == cqltypes.TupleType:
            new_name = lower_name + '_ss'
        else:
            new_name = lower_name + '_s'

        search[new_name] = value

    return search


