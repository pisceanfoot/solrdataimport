# -*- coding:utf-8 -*-
import traceback

def format_exc():
	return traceback.format_exc()


if __name__ == '__main__':
	try:
		caa()
		raise Exception('1231232')
		a = 1/0
	except:
		print format_exc()