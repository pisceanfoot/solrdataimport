
Data import from cassandra to Solr and ElasticSearch
=========================================

SolrDataImport is use to export data from cassandra to solr, now also support elastic search. SolrDataImport simulate inner query and sub query in code, that meas which can `inner join` cassandra column family, or combine a row as a new field in parent table.


Example
------------------------

```
if __name__ == '__main__':
    import logging.config
    logging.basicConfig(level=logging.ERROR,
                        format='%(levelname)-s: %(message)s')

    setting = {
        "config_file": "/fil_epath/test.json",
        "solr_url": "http://ip:8983/solr/",
        "solr_cluster": True or False,
        "cassandra": {
            "hosts": ["ip"],
            "username": "username",
            "password": "password"
        }
    }
    dataimport = DataImport(setting)
    dataimport.export('userinfo', **{"userId": "27aa99c7-3673-4602-95a9-ce0620b51695"})
    dataimport.delete('game_info', **{"game_id": "30ae2131-87d4-4f3c-802d-b671ddeb6648"})
```


Nest mode
------------------------

table 1

| use_id | user_name | create_date         |
| ------ | --------- | ------------------- |
| 1      | hello     | 2017-01-01 00:00:00 |
| 2      | World     | 2017-01-02 00:00:00 |


table 2

| use_id | age  | create_date         |
| ------ | ---- | ------------------- |
| 1      | 20   | 2017-01-01 00:00:00 |
| 2      | 29   | 2017-01-02 00:00:00 |

nest result is just like use `select * from table1 inner join table2 where table1.user_id = table2.user_id`

result

| use_id | user_name | age  | create_date         |
| ------ | --------- | :--: | ------------------- |
| 1      | hello     |  20  | 2017-01-01 00:00:00 |
| 2      | World     |  29  | 2017-01-02 00:00:00 |


Combine mode
-------------------------------------

table 1

| use_id | user_name | create_date         |
| ------ | --------- | ------------------- |
| 1      | hello     | 2017-01-01 00:00:00 |
| 2      | World     | 2017-01-02 00:00:00 |


table 2

| use_id | age  | create_date         |
| ------ | ---- | ------------------- |
| 1      | 20   | 2017-01-01 00:00:00 |
| 2      | 29   | 2017-01-02 00:00:00 |

nest result is just like use `select *, (select * from table2) as new_field from table1 where table1.user_id = table2.user_id`, new_field can be a json object, json array or JSON.stringfy.

result

| use_id | user_name | new_field     | create_date         |
| ------ | --------- | ------------- | ------------------- |
| 1      | hello     | {json_object} | 2017-01-01 00:00:00 |
| 2      | World     | {json_object} | 2017-01-02 00:00:00 |


Program setting
--------------------------------------------------

```
{
  "cassandra": {
      "hosts": ["ip"],
      "username": "username",
      "password": "password"
  },
  "fullDataImport": False,
  "config_file": "/path/config.json"

  # solr setting
  "solr_url": "http://ip:8983/solr/",
  "cluster": False,
  "commitWithin": 1000,

  # es setting
  "elastic_endpoints": [elastic endpoints],
  "index_template": {
    "index_name": "tamplate name"
  },
  "rebuild_all": False
}
```

- cassandra: setting cassandra seeds, username and password if present.
- fullDataImport: default False. If True, this will load all data from column family and export to solr or es. For solr it will remove all data first.
- solr_url: sorl url.
- cluster: default False.
- commitWithin: default 1000ms.
- elastic_endpoints: elastic search endpoints. **ES only**.
- Index_template. Default None. If set it will create index with this index template setting. **ES only**.
- rebuild_all: It effect only when fullDataImport is set to True. It will create a new index and use pre name as a alias.  **ES only**.



Export config
------------------------------------------

**config_file** detail:


```
{
        "name": "key of the config, and also set as solr core name if core_name not present",
        "core_name": "solr core name",
        "index_name": "index name for elastic search",
        "type_name": "type name for elastic search",
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
        "combine": [{ # combine result will set as a JSON field in parent doc
                      # one record will set like "name": {}
                      # mutil records will set as "name": [{}]
                      # also can set as "name": JSON.stringify(...)
            "table": "table name",
            "combineKey": { # same as nestKey
                "nest_table_key": "parent_table_key", # select * from table_parent inner join this_table where this_table.nest_table_key = table_parent.parent_table_key
                "nest_table_key2": "parent_table_key2"
            },
            "field_name": "new field name",
            "field_type": "string",  # string or object
            "field_map_one2one": True,
            "cache": Ture or False # nest table can be cachable
            "condition": {
                "filed": "value"  # field should equals to value
            }
        }],
        "documentId": ["value for solr _id"],
        "documentField":["solr filed"],
        "exclude": ["field name"]
    }
```

code
--------------------

```
from solrdataimport import DataImport
dataimport = DataImport(setting)
    dataimport.export('userinfo', **{"userId": "27aa99c7-3673-4602-95a9-ce0620b51695"})
    dataimport.delete('game_info', **{"game_id": "30ae2131-87d4-4f3c-802d-b671ddeb6648"})
```

- export, load row(s) and send to solr or es
    * name: **name*** in config_file
    * args: export row(s) where match **key** setting in config_file. None for fulldataimport.
- delete, delete row(s) which math args
    * name: **name*** in config_file
    * args: export row(s) where match **key** setting in config_file. 


config example
--------------------------

```
[{
  "name": "user_info",
  "table": "user_info",
  "key": ["userId"],
  "nest": [{
    "table": "user_detail",
    "nestKey": {
      "userId": "userId"
    }
  }],
  "documentId": ["userId"]
},{
  "name": "user_info2",
  "core_name": "user_info",
  "table": "user_info",
  "key": ["userId"],
  "nest": [{
    "table": "user_detail",
    "nestKey": {
      "userId": "userId"
    },
    "condition": {
      "team_id": "1"
    },
    "alias": {
      "team_id": "new_team_id"
    }
  }],
  "documentId": ["userId", "new_team_id"]
}]

```