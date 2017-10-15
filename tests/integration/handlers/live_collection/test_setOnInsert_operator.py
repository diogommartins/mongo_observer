import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class SetOnInsertOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                               asynctest.TestCase):
    """
    $setOnInsert

    If an update operation with upsert: true results in an insert of a document,
     then $setOnInsert assigns the specified values to the fields in the
     document. If the update operation does not result in an insert,
     $setOnInsert does nothing.

    You can specify the upsert option for either the db.collection.update()
    or db.collection.findAndModify() methods.
    """

    async def test_it_sets_value_if_a_document_is_created(self):
        await self.collection.update_one(
            {"_id": 1},
            {
                "$set": {"item": "apple"},
                "$setOnInsert": {"defaultQty": 100}
            },
            upsert=True
        )
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection[1],
                         {'_id': 1, 'item': 'apple', 'defaultQty': 100})

    async def test_it_doesnt_sets_value_if_a_documents_isnt_created(self):
        await self.collection.update_one(
            {"_id": self.doc['_id']},
            {
                "$set": {"item": "apple"},
                "$setOnInsert": {"defaultQty": 100}
            },
            upsert=True
        )
        await self.observer.observe_changes()

        self.assertNotIn("defaultQty", self.handler.collection[self.doc['_id']])
        self.assertIn(self.handler.collection[self.doc['_id']]["item"], "apple")
