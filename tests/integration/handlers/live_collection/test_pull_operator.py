import asynctest
import pymongo

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class PullOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                        asynctest.TestCase):
    """
    $pull

    The $pull operator removes from an existing array all instances of a value
    or values that match a specified condition.
    """
    template_doc = {
        "fruits": ["apples", "pears", "oranges", "grapes", "bananas"],
        "vegetables": ["carrots", "celery", "squash", "carrots"],
        "votes": [3, 5, 6, 7, 7, 8],
        "results": [
            {"item": "A", "score": 5},
            {"item": "B", "score": 8, "comment": "Strongly agree"}
        ]
    }

    async def test_pull_operators_gets_converted_into_set_operator(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {'$pull': {'vegetables': "carrots"}}
        )

        with asynctest.patch.object(self.observer.operation_handler,
                                    'handle') as handle:
            await self.observer.observe_changes()
            operation = handle.call_args_list[1][1]['operation']
            self.assertIn("$set", operation['o'])

    async def test_it_removes_from_multiple_fields_at_once(self):
        await self.collection.update(
            {'_id': self.doc['_id']},
            {
                '$pull': {
                    'fruits': { '$in': ["apples", "oranges"]},
                    'vegetables': "carrots"}
            }
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "fruits": ["pears", "grapes", "bananas"],
                "vegetables": ["celery", "squash"],
                "votes": [3, 5, 6, 7, 7, 8],
                "results": [
                    {"item": "A", "score": 5},
                    {"item": "B", "score": 8, "comment": "Strongly agree"}
                ]
            }
        )

    async def test_it_removes_items_matching_specified_condition(self):
        await self.collection.update(
            {'_id': self.doc['_id']},
            {'$pull': {'votes': { '$gte': 6}}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "fruits": ["apples", "pears", "oranges", "grapes", "bananas"],
                "vegetables": ["carrots", "celery", "squash", "carrots"],
                "votes": [3, 5],
                "results": [
                    {"item": "A", "score": 5},
                    {"item": "B", "score": 8, "comment": "Strongly agree"}
                ]
            }
        )

    async def test_it_removes_from_array_of_documents(self):
        await self.collection.update(
            {'_id': self.doc['_id']},
            {
                '$pull': {
                    'results': {
                        'score': 8,
                        'item': "B"
                    }
                }
            }
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "fruits": ["apples", "pears", "oranges", "grapes", "bananas"],
                "vegetables": ["carrots", "celery", "squash", "carrots"],
                "votes": [3, 5, 6, 7, 7, 8],
                "results": [
                    {"item": "A", "score": 5}
                ]
            }
        )
