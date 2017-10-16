import asynctest
import pymongo

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class PopOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                            asynctest.TestCase):
    """
    $pop
    The $pop operator removes the first or last element of an array. Pass $pop a
    value of -1 to remove the first element of an array and 1 to remove the
    last element in an array.

    Behavior

    * The $pop operation fails if the <field> is not an array.

    * If the $pop operator removes the last item in the <field>, the <field>
    will then hold an empty array.


    """
    template_doc = {
        "letters": ["a", "b"],
        "dog": "Xablau"
    }

    async def test_it_remove_first_item_of_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$pop": {"letters": -1}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["b"],
                "dog": "Xablau"
            }
        )

    async def test_it_remove_last_item_of_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$pop": {"letters": 1}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a"],
                "dog": "Xablau"
            }
        )

    async def test_it_fails_if_not_array_field(self):
        with self.assertRaises(pymongo.errors.WriteError):
            await self.collection.update(
                {"_id": self.doc['_id']},
                {"$pop": {"dog": 1}}
            )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a", "b"],
                "dog": "Xablau"
            }
        )

    async def test_pop_operators_gets_converted_into_set_operator(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$pop": {"letters": 1}}
        )

        with asynctest.patch.object(self.observer.operation_handler, 'handle') as handle:
            await self.observer.observe_changes()
            operation = handle.call_args_list[1][1]['operation']
            self.assertIn("$set", operation['o'])
