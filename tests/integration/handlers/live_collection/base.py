from uuid import uuid4

from tests.integration.handlers.base import BaseLiveCollectionTests


class LiveCollectionFieldUpdateOperatorsTests(BaseLiveCollectionTests):
    template_doc = {"dog": "Xablau", 'random_unique_id': uuid4().hex}

    async def setUp(self):
        await super(LiveCollectionFieldUpdateOperatorsTests, self).setUp()
        self.doc = self.template_doc.copy()
        await self.collection.insert_one(self.doc)
