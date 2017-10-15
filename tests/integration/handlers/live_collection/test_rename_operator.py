import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


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