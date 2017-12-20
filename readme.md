Data import from cassandra to Solr
=========================================


debug
------------------------


```
if __name__ == '__main__':
    import logging.config
    logging.basicConfig(level=logging.ERROR,
                        format='%(levelname)-s: %(message)s')

    
    setting = {
        "fullDataImport": False,
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
    # dataimport.exportSolr('userinfo', **{"userId": "27aa99c7-3673-4602-95a9-ce0620b51695"})
    # dataimport.deleteSolr('game_info', **{"game_id": "30ae2131-87d4-4f3c-802d-b671ddeb6648"})
```

config
----------------------

```
[{
    "name": "userinfo",
    "table": "userinfo",
    "key": ["userId"],
    "nest": [{
        "table": "sso.member",
        "nestKey": {
            "member_id": "userId"
        },
        "cache": true
    }],
    "exclude": ["member_id"],
    "solrId": ["member_id"]
},{
    "name": "hb.game_epic",
    "core_name": "userinfo",
    "table": "hb.game_epic",
    "key": ["epic_id"],
    "nest": [{
        "table": "hb.epic_member_map",
        "nestKey": {
            "epic_id": "epic_id"
        },
        "condition": {
            "team_id": "b39ba1e9-4505-49ce-92d7-92c4b77d9923"
        },
        "alias": {
            "epic_id": "team_epic_id"
        }
    }],
    "solrId": ["epic_id", "member_id"]，
    "solrKey": [epic_id", "member_id", "xid", "xkey"] # Option
}]
```