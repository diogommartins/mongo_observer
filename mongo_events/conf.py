from os import environ as env

from simple_json_logger import JsonLogger

MONGO_URI = env.get("SIEVE_MONGO_URI", "mongodb://127.0.0.1")
MONGO_MAX_POOLSIZE = int(env.get("SIEVE_MONGO_MAX_POOLSIZE", 20))
MONGO_SERVER_SELECTION_TIMEOUT = int(env.get("SIEVE_MONGO_SERVER_SELECTION_TIMEOUT",
                                             5000))
MONGO_CONN_PARAMS = dict(
    host=MONGO_URI,
    maxPoolSize=MONGO_MAX_POOLSIZE,
    serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT
)


SLEEP_TIME_BEFORE_GET_NEW_CURSOR_IN_SECONDS = float(env.get('SLEEP_TIME_BEFORE_GET_NEW_CURSOR_IN_SECONDS',
                                                          0.1))

OPLOG_DATABASE = env.get('OPLOG_DATABASE', 'local')
OPLOG_COLLECTION = env.get('OPLOG_COLLECTION', 'oplog.$main')

SELLER_DATABASE = env.get('SELLER_DATABASE', 'seller')

logger = JsonLogger()
