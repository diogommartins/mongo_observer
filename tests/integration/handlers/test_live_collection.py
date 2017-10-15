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


class MinOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                       asynctest.TestCase):
    template_doc = {"highScore": 800, "lowScore": 200}

    async def test_it_updates_values_higher_than_min(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$min": {
                "lowScore": 150
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 800, "lowScore": 150}
        )

    async def test_it_does_nothing_to_values_lower_than_min(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$min": {
                "lowScore": 250
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 800, "lowScore": 200}
        )

    async def test_an_update_operation_with_min_higher_than_value_doesnt_result_into_a_oplog_entry(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$min": {
                "lowScore": 250
            }
        })

        with asynctest.patch.object(self.observer.operation_handler,
                                    'on_update') as on_update:
            await self.observer.observe_changes()
            on_update.assert_not_called()


class MaxOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                       asynctest.TestCase):
    template_doc = {"highScore": 800, "lowScore": 200}

    async def test_it_updates_values_lower_than_max(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$max": {
                "highScore": 950
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 950, "lowScore": 200}
        )

    async def test_it_does_nothing_to_values_higher_than_max(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$max": {
                "highScore": 700
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 800, "lowScore": 200}
        )

    async def test_an_update_operation_with_max_lower_than_value_doesnt_result_into_a_oplog_entry(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$max": {
                "highScore": 700
            }
        })

        with asynctest.patch.object(self.observer.operation_handler,
                                    'on_update') as on_update:
            await self.observer.observe_changes()
            on_update.assert_not_called()


class MulOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                       asynctest.TestCase):
    """
    $mul

    Multiply the value of a field by a number. The field to update must contain
    a numeric value.

    Missing Field

    If the field does not exist in a document, $mul creates the field and
    sets the value to zero of the same numeric type as the multiplier.

    Mixed Type

    Multiplication with values of mixed numeric types (32-bit integer,
    64-bit integer, float) may result in conversion of numeric type. For
    multiplication with values of mixed numeric types, the following type
    conversion rules apply:
    """
    template_doc = {"dog": "Xablau", "price": 10.99}

    async def test_it_multiply_the_value_of_a_field(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$mul": {
                "price": 1.25
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "dog": "Xablau", "price": 13.7375}
        )

    async def test_using_mul_to_non_existing_field_created_it_with_zero_value(self):
        await self.collection.update_one({'_id': self.doc['_id']},
                                         {"$unset": {"price": ""}})
        await self.observer.observe_changes()
        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "dog": "Xablau"}
        )

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$mul": {
                "price": 1.25
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "dog": "Xablau", "price": 0}
        )
