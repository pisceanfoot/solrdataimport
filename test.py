from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra import cqltypes
from cassandra.concurrent import execute_concurrent
import uuid

import datetime

print(datetime.datetime.now())

param={}
param['auth_provider'] = PlainTextAuthProvider(username='grunt', password='HCDhcd123')
cluster = Cluster(["10.20.32.75"], **param)
session = cluster.connect('system')

# select_statement = session.prepare("SELECT * FROM mars_bp.game_epic_member WHERE epic_id=?")

statements_and_params = []
# for user_id in user_ids:
#     params = (user_id, )
#     statements_and_params.append((select_statement, params))

for x in range(0, 109):
    statements_and_params.append(('SELECT * FROM mars_bp.game_epic_member WHERE epic_id=?', [uuid.UUID('d86965ee-f6a8-41e0-a620-45dd5e8cd0ab')]))

for x,d in statements_and_params:
    print(x,d)
    # break
    pcql = session.prepare(x)
    pcql.consistency_level = ConsistencyLevel.ONE
    session.execute(pcql, d)

# results = execute_concurrent(
#     session, statements_and_params, raise_on_first_error=False)

# index = 0
# for (success, result) in results:
#     if not success:
#         print('errr')
#     else:
#         print('ok')
#     print(str(index))
#     index = index +1 

print(datetime.datetime.now())