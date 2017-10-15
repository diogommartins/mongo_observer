import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


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