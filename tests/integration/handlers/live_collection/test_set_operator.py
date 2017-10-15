import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


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