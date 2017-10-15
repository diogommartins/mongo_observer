from uuid import uuid4

import asynctest

from tests.integration.handlers.live_collection.base import BaseLiveCollectionTests


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