import asynctest
import pymongo

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class PullAllOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                           asynctest.TestCase):
    """
    $pullAll

    The $pullAll operator removes all instances of the specified values from
    an existing array. Unlike the $pull operator that removes elements by
    specifying a query, $pullAll removes elements that match the listed values.

    Behavior

    * If a <value> to remove is a document or an array, $pullAll removes
    only the elements in the array that match the specified <value> exactly,
    including order.
    """
    template_doc = {
        'scores': [0, 2, 5, 5, 1, 0]
    }

    async def test_it_removes_multiple_instances_of_the_same_value(self):
        await self.collection.update_one(
            {'_id': self.doc['_id']},
            {
                "$pullAll": {
                    "scores": [0, 5]
                }
            }
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "scores": [2, 1]
            }
        )

    async def test_pullall_operators_gets_converted_into_set_operator(self):
        await self.collection.update(
            {'_id': self.doc['_id']},
            {
                "$pullAll": {
                    "scores": [0, 5]
                }
            }
        )

        with asynctest.patch.object(self.observer.operation_handler,
                                    'handle') as handle:
            await self.observer.observe_changes()
            operation = handle.call_args_list[1][1]['operation']
            self.assertIn("$set", operation['o'])
