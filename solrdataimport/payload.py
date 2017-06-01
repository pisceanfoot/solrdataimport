# -*- coding:utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals, \
    with_statement
import json
from solrdataimport.map import Map

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
