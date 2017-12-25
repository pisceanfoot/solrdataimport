# from elasticsearch import Elasticsearch
# es = Elasticsearch(['10.20.32.137:9200'])

# a = es.indices.get_template('hello_leo', ignore=[404])
# print(a)

# a = es.index('o3333', doc_type='test', body={
#     "hello":'2',
#     "a": 3,
#     "b": 4
#     }, id=3)

# print(a)

# a = es.search(index = 'o3333', doc_type='test', q="hello:\"2\"")
# print(a)
# a = es.search(index = 'hello_leo', doc_type='test', q="hello:\"2\"")
# print(a)

# a = es.indices.create('hello_leo', ignore=[400])
# print(a)

# a = es.indices.delete('hello_leo', ignore=[404,400])
# print(a)

# a = es.indices.exists('hello_leo', ignore=[404,400])
# print(a)

# a = es.indices.get_alias(index='hello*', ignore=[404])
# print(a)
# a = es.indices.get_alias(index='hello1*', ignore=[404])
# print(a)

# name = 'hello_leo'
# if not a.has_key('error'):

# a = es.indices.put_alias(index = 'hello_leo', name = 'o3333')
# print(a)

# a = es.indices.delete_alias(index = 'hello_leo', name = '_all', ignore=[404])
# print(a)

# import json
# set_al = [1,2]
# print json.dumps(set_al)

a = {"a":1}
for x in a:
    print a[x]
