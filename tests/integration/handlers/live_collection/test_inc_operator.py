import asynctest
import pymongo

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class IncOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                        asynctest.TestCase):
    """
    $inc
    The $inc operator increments a field by a specified value and has
    the following form:
        `{ $inc: { <field1>: <amount1>, <field2>: <amount2>, ... } }`

    Behavior

    * The $inc operator accepts positive and negative values.
    * If the field does not exist, $inc creates the field and sets the field to
    the specified value.
    * Use of the $inc operator on a field with a null value will generate an error.
    * $inc is an atomic operation within a single document.
    """
    template_doc = {
        "sku": "Xablau",
        "quantity": 10,
        "metrics": {
            "orders": 2,
            "ratings": 3.5
        }
    }

    async def test_it_increments_existing_values(self):
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

    async def test_inc_on_null_value_field(self):
        await self.collection.update_one({"_id": self.doc['_id']},
                                         {"$set": {"sku": None}})

        with self.assertRaises(pymongo.errors.WriteError):
            await self.collection.update_one({"_id": self.doc['_id']},
                                             {"$inc": {"sku": 666}})

    async def test_it_increments_existing_nested_values(self):
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
