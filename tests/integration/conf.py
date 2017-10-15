from os import environ as env


MONGO_URI = env.get("TEST_MONGO_URI", "mongodb://127.0.0.1")
MONGO_MAX_POOLSIZE = int(env.get("TEST_MONGO_MAX_POOLSIZE", 20))
MONGO_SERVER_SELECTION_TIMEOUT = int(env.get("TEST_MONGO_SERVER_SELECTION_TIMEOUT",
                                             5000))
MONGO_CONN_PARAMS = dict(
    host=MONGO_URI,
    maxPoolSize=MONGO_MAX_POOLSIZE,
    serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT
)

OPLOG_DATABASE = env.get('OPLOG_DATABASE', 'local')
OPLOG_COLLECTION = env.get('OPLOG_COLLECTION', 'oplog.rs')

TEST_DATABASE = env.get('TEST_DATABASE', 'xablau')
TEST_COLLECTION = env.get('TEST_COLLECTION', 'xablau_collection')
