from uuid import uuid4

from motor.motor_asyncio import AsyncIOMotorClient

from mongo_observer.observer import Observer, ShouldStopObservation
from mongo_observer.operation_handlers import LiveCollection
from tests.integration import conf


class BaseLiveCollectionTests:
    async def setUp(self):
        self.client = AsyncIOMotorClient(**conf.MONGO_CONN_PARAMS)
        self.oplog_db = self.client[conf.OPLOG_DATABASE]
        self.oplog_collection = self.oplog_db[conf.OPLOG_COLLECTION]
        self.db = self.client[conf.TEST_DATABASE]
        self.collection = self.db[conf.TEST_COLLECTION]

        self.handler = await LiveCollection.init_async(self.collection)
        self.observer = await Observer.init_async(
            oplog=self.oplog_collection,
            operation_handler=self.handler,
            namespace_filter=f'{self.db.name}.{self.collection.name}',
            on_nothing_to_fetch_on_cursor=self.stop_infinite_iteration
        )
        await self.observer.observe_changes()

    async def tearDown(self):
        await self.collection.delete_many({})

    async def stop_infinite_iteration(self):
        raise ShouldStopObservation


class LiveCollectionFieldUpdateOperatorsTests(BaseLiveCollectionTests):
    template_doc = {"dog": "Xablau", 'random_unique_id': uuid4().hex}

    async def setUp(self):
        await super(LiveCollectionFieldUpdateOperatorsTests, self).setUp()
        self.doc = self.template_doc.copy()
        await self.collection.insert_one(self.doc)
