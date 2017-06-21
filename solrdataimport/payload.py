# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement

import json
from solrdataimport.lib.map import Map

class Payload:
	"""
	load section list from json file

	Payload.load('json_file_path')

	{
		"name": "key of the config, and also set as solr core name if core_name not present",
		"core_name": "solr core name",
		"table": "cassandra table name (keyspace.table), will use in cql like select * from table.",
		"key": ["table key and partition key list"],
		"nest": [{
			"table": "table name, use like select * from table_parent inner join this_table",
			"nestKey": {
				"nest_table_key": "parent_table_key", # select * from table_parent inner join this_table where this_table.nest_table_key = table_parent.parent_table_key
				"nest_table_key2": "parent_table_key2"
			},
			"cache": Ture or False # nest table can be cachable
			"condition": {
				"filed": "value"  # field should equals to value
			},
			"alias": {
	            "field": "new name"
	        }
		}],
		"solrId": ["value for solr _id"],
	  	"solrKey":["solr filed"],
	  	"exclude": ["field name"]
	}
	"""
	sectionList=None

	@classmethod
	def load(cls, config_file):
		sectionList = []
		with open(config_file) as f:
			jsonObject = json.load(f, encoding='utf-8')
			if jsonObject:
				for section in jsonObject:
					section_map = Map(section)
					if section_map.nest:
						array = []
						for nest in section_map.nest:
							section_nest = Map(nest)

							if section_nest.condition:
								section_nest.condition = lower_case_dict(section_nest, 'condition')
							if section_nest.alias:
								section_nest.alias = lower_case_dict(section_nest, 'alias', value_lower=True)

							array.append(section_nest)
						section_map.nest = array

					if section_map.exclude:
						section_map.exclude = map(lower_case, section_map.exclude)
					if section_map.solrId:
						section_map.solrId = map(lower_case, section_map.solrId)
					if section_map.solrKey:
						section_map.solrKey = map(lower_case, section_map.solrKey)
					if section_map.condition:
						section_map.condition = lower_case_dict(section_map, 'condition')
					if section_map.alias:
						section_map.alias = lower_case_dict(section_map, 'alias', value_lower=True)

					sectionList.append(section_map)

		cls.sectionList = sectionList

	@classmethod
	def get(cls, name):
		for x in cls.sectionList:
			if x.name == name:
				return x

		return None


def lower_case(x):
	return x.lower()

def lower_case_dict(section_map, field, value_lower=False):
	value = section_map.get(field)
	if not value:
		return value

	new_dic = {}
	for x in value:
		data = value[x]
		if value_lower and hasattr(data, 'lower'):
			data = data.lower()

		new_dic[x.lower()] = data

	return new_dic


if __name__ == '__main__':
	config_file='/Users/leo/Documents/Workspace/OpenDev/solrdataimport/test.json'
	Payload.load(config_file)
	print(Payload.sectionList)
	# print(Payload.get('01_load.userinfo'))
