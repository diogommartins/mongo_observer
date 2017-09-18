import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

from mongo_observer import conf
from mongo_observer.observer import Observer
from mongo_observer.operation_handlers import BuyboxChangeNotifier


async def run():
    client = AsyncIOMotorClient(**conf.MONGO_CONN_PARAMS)
    oplog_db = client[conf.OPLOG_DATABASE]
    seller_db = client[conf.SELLER_DATABASE]

    handler = await BuyboxChangeNotifier.init_async(seller_db['price_rank'])
    # now = bson.Timestamp(datetime.now(), 1)
    observer = await Observer.init_async(oplog=oplog_db[conf.OPLOG_COLLECTION],
                                         operation_handler=handler,
                                         namespace_filter='seller.price_rank')
    await observer.observe_changes()


loop = asyncio.get_event_loop()
loop.run_until_complete(run())
