import pymongo


class MongoHandler:
    def __init__(self, uri="mongodb://root:root@localhost:27017", db_name="SP500_DB"):
        self.db = db_name
        self.uri = uri
        self.client = None

    def connect_to_mongo(self):
        try:
            self.client = pymongo.MongoClient(self.uri, maxPoolSize=300)
            self.client.server_info()  # force connection on a request as the
            # connect=True parameter of MongoClient seems
            # to be useless here
        except Exception as e:
            return None

        return pymongo.MongoClient(self.uri, maxPoolSize=300)

    def close_mongo_connection(self):
        self.client.close()

    def get_database(self):
        return self.client[self.db]
