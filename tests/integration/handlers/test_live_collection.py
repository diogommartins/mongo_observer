from uuid import uuid4

import asynctest

from motor.motor_asyncio import AsyncIOMotorClient

from mongo_observer.observer import Observer, ShouldStopObservation
from mongo_observer.operation_handlers import LiveCollection
from tests.integration import conf


class LiveCollectionTests(asynctest.TestCase):
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

    async def tearDown(self):
        await self.collection.delete_many({})

    async def stop_infinite_iteration(self):
        raise ShouldStopObservation

    async def observe_and_stop(self):
        with self.assertRaises(ShouldStopObservation):
            await self.observer.observe_changes()

    async def test_collection_starts_empty(self):
        self.assertEqual(self.handler.collection, {})

    async def test_it_reacts_to_insert_operations(self):
        doc = {"candy": "Minhocas Fini", 'rid': uuid4().hex}
        await self.collection.insert_one(doc)

        await self.observe_and_stop()
        self.assertEqual(self.handler.collection, {doc['_id']: doc})

    async def test_it_reacts_to_delete_operations(self):
        doc = {"candy": "Minhocas Fini", 'rid': uuid4().hex}
        await self.collection.insert_one(doc)

        await self.observe_and_stop()
        self.assertEqual(self.handler.collection, {doc['_id']: doc})

        await self.collection.delete_one({"_id": doc['_id']})

        await self.observe_and_stop()
        self.assertEqual(self.handler.collection, {})

    async def test_it_reacts_to_set_update_operations(self):
        doc = {"candy": "Minhocas Fini", 'rid': uuid4().hex}
        await self.collection.insert_one(doc)

        await self.observe_and_stop()
        self.assertEqual(self.handler.collection, {doc['_id']: doc})

        await self.collection.update_one({'_id': doc['_id']}, {
            "$set": {
                "candy": "Ursinhos Fini"
            }
        })

        await self.observe_and_stop()
        self.assertEqual(self.handler.collection[doc['_id']]['candy'],
                         "Ursinhos Fini")
