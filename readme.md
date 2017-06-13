Data import from cassandra to Solr
=========================================


debug
------------------------

if __name__ == '__main__':
    import logging.config
    logging.basicConfig(level=logging.ERROR,
                        format='%(levelname)-s: %(message)s')

    from solrdataimport.dataload.cassClient import CassandraClient
    CassandraClient.init(["ip"],username="username",password="password")

    setting = {
        "fullDataImport": False,
        "config_file": "/fil_epath/test.json",
        "solr_url": "http://ip:8983/solr/"
    }
    dataimport = DataImport(setting)
    # dataimport.exportSolr('userinfo', **{"userId": "27aa99c7-3673-4602-95a9-ce0620b51695"})
    # dataimport.deleteSolr('game_info', **{"game_id": "30ae2131-87d4-4f3c-802d-b671ddeb6648"})

