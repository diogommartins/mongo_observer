import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class BitwiseOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                            asynctest.TestCase):
    """
    $bit

    The $bit operator performs a bitwise update of a field. The operator
    supports bitwise and, bitwise or, and bitwise xor (i.e. exclusive or)
    operations.

    Only use this operator with integer fields (either 32-bit integer or
    64-bit integer).
    """
    template_doc = {"expdata": 13, "dog": "Xablau"}

    async def test_bitwise_and(self):
        await self.collection.update_one(
            {"_id": self.doc['_id']},
            {
                "$bit": {
                    "expdata": {"and": 10}
                }
            }
        )
        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                '_id': self.doc['_id'],
                'expdata': 8,
                'dog': 'Xablau'
            }
        )

    async def test_bitwise_or(self):
        await self.collection.update_one(
            {"_id": self.doc['_id']},
            {
                "$bit": {
                    "expdata": {"or": 10}
                }
            }
        )
        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                '_id': self.doc['_id'],
                'expdata': 15,
                'dog': 'Xablau'
            }
        )

    async def test_bitwise_xor(self):
        await self.collection.update_one(
            {"_id": self.doc['_id']},
            {
                "$bit": {
                    "expdata": {"and": 10}
                }
            }
        )
        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                '_id': self.doc['_id'],
                'expdata': 8,
                'dog': 'Xablau'
            }
        )

    async def test_bitwise_operators_gets_converted_into_set_operator(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {
                "$bit": {
                    "expdata": {"or": 10}
                }
            }
        )

        with asynctest.patch.object(self.observer.operation_handler,
                                    'handle') as handle:
            await self.observer.observe_changes()
            operation = handle.call_args_list[1][1]['operation']
            self.assertIn("$set", operation['o'])
