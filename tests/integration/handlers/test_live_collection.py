from uuid import uuid4

import asynctest

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

    async def tearDown(self):
        await self.collection.delete_many({})

    async def stop_infinite_iteration(self):
        raise ShouldStopObservation


class LiveCollectionTests(BaseLiveCollectionTests, asynctest.TestCase):
    async def test_collection_starts_empty(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {})

    async def test_it_reacts_to_insert_operations(self):
        doc = {"candy": "Minhocas Fini", 'rid': uuid4().hex}
        await self.collection.insert_one(doc)

        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {doc['_id']: doc})

    async def test_it_reacts_to_delete_operations(self):
        doc = {"candy": "Minhocas Fini", 'rid': uuid4().hex}
        await self.collection.insert_one(doc)

        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {doc['_id']: doc})

        await self.collection.delete_one({"_id": doc['_id']})

        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {})


class LiveCollectionFieldUpdateOperatorsTests(BaseLiveCollectionTests):
    template_doc = {"dog": "Xablau", 'random_unique_id': uuid4().hex}

    async def setUp(self):
        await super(LiveCollectionFieldUpdateOperatorsTests, self).setUp()
        self.doc = self.template_doc.copy()
        await self.collection.insert_one(self.doc)


class SetOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                        asynctest.TestCase):
    async def test_it_reacts_to_set_update_operations(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$set": {
                "dog": "Xena"
            }
        })

        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection[self.doc['_id']]['dog'],
                         "Xena")


class UnsetOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                          asynctest.TestCase):
    pass


class RenameOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                           asynctest.TestCase):
    """
    A $rename operation is actually a $set and $unset operation
    """
    async def test_it_renames_document_key_if_one_exists(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$rename": {
                "dog": "cat"
            }
        })

        await self.observer.observe_changes()

        with self.assertRaises(KeyError):
            self.handler.collection[self.doc['_id']]['dog']

        self.assertEqual(self.handler.collection[self.doc['_id']]['cat'], "Xablau")

    async def test_it_doesnt_renames_document_key_if_one_doesnt_exists(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$rename": {
                "cat": "dog"
            }
        })

        await self.observer.observe_changes()

    async def test_it_renames_multiple_keys(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$rename": {
                "dog": "cat",
                "random_unique_id": "rid"
            }
        })

        await self.observer.observe_changes()

        with self.assertRaises(KeyError):
            self.handler.collection[self.doc['_id']]['dog']

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                '_id': self.doc['_id'],
                'cat': self.doc['dog'],
                'rid': self.doc['random_unique_id']
            }
        )


class IncOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                        asynctest.TestCase):
    template_doc = {
        "sku": "Xablau",
        "quantity": 10,
        "metrics": {
            "orders": 2,
            "ratings": 3.5
        }
    }

    async def test_it_increments_existing_values(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection, {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$inc": {
                "quantity": -2
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "sku": "Xablau",
                "quantity": 8,
                "metrics": {
                    "orders": 2,
                    "ratings": 3.5
                }
            }
        )

    async def test_it_increments_existing_nested_values(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$inc": {
                "metrics.orders": 10
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "sku": "Xablau",
                "quantity": 10,
                "metrics": {
                    "orders": 12,
                    "ratings": 3.5
                }
            }
        )

    async def test_it_creates_nonexisting_values(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$inc": {
                "metrics.likes": 666
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "sku": "Xablau",
                "quantity": 10,
                "metrics": {
                    "orders": 2,
                    "ratings": 3.5,
                    "likes": 666
                }
            }
        )
