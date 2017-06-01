# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement
import json
from solrdataimport.map import Map

def lower_case(x):
	return x.lower()

class Payload:
	"""
	load section list from json file

	Payload.load('json_file_path')
	"""
	sectionList=None

	@classmethod
	def load(cls, config_file):
		sectionList = []
		with open(config_file) as f:
			jsonObject = json.load(f, encoding='utf-8')
			if jsonObject:
				for section in jsonObject:
					sectionList.append(Map(section))

		for section in sectionList:
			if section.exclude:
				section.exclude = map(lower_case, section.exclude)
			if section.solrId:
				section.solrId = map(lower_case, section.solrId)
			if section.solrKey:
				section.solrKey = map(lower_case, section.solrKey)

		cls.sectionList = sectionList

	@classmethod
	def get(cls, name):
		for x in cls.sectionList:
			if x.name == name:
				return x

		return None

if __name__ == '__main__':
	config_file='/Users/leo/Documents/Workspace/OpenDev/solrdataimport/test.json'
	Payload.load(config_file)
	# print(Payload.get('01_load.userinfo'))
