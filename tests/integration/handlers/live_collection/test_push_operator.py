import asynctest
import pymongo

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class PushOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                        asynctest.TestCase):
    """
    $push

    The $push operator appends a specified value to an array.

    Behavior

    * If the field is absent in the document to update, $push adds the array
    field with the value as its element.

    * If the field is not an array, the operation will fail.

    * If the value is an array, $push appends the whole array as a single
    element. To add each element of the value separately, use the $each modifier
     with $push. For an example, see Append Multiple Values to an Array. For a
     list of modifiers available for $push, see Modifiers.

    """
    template_doc = {
        "letters": ["a", "b"],
        "dog": "Xablau"
    }

    async def test_push_operators_gets_converted_into_set_operator(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$push": {"letters": "c"}}
        )

        with asynctest.patch.object(self.observer.operation_handler,
                                    'handle') as handle:
            await self.observer.observe_changes()
            operation = handle.call_args_list[1][1]['operation']
            self.assertIn("$set", operation['o'])

    async def test_it_pushes_to_existing_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$push": {"letters": "a"}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a", "b", "a"],
                "dog": "Xablau"
            }
        )

    async def test_it_pushes_array_values_to_existing_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$push": {"letters": ["a", "b"]}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a", "b", ["a", "b"]],
                "dog": "Xablau"
            }
        )

    async def test_it_pushes_multiple_values_to_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$push": {"letters": {"$each": ["a", "b"]}}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a", "b", "a", "b"],
                "dog": "Xablau"
            }
        )

    async def test_it_fails_to_append_to_non_array_field(self):
        with self.assertRaises(pymongo.errors.WriteError):
            await self.collection.update(
                {"_id": self.doc['_id']},
                {"$push": {"dog": "c"}}
            )

        await self.observer.observe_changes()

        self.assertEqual(self.handler.collection[self.doc['_id']], self.doc)


class PushOperatorModifiersTests(LiveCollectionFieldUpdateOperatorsTests,
                                 asynctest.TestCase):
    template_doc = {
        "quizzes": [
            {"wk": 1, "score": 10},
            {"wk": 2, "score": 8},
            {"wk": 3, "score": 5},
            {"wk": 4, "score": 6}
        ]
    }

    async def test_all_modifiers(self):
        """
        The following $push operation uses:

        * the $each modifier to add multiple documents to the quizzes array,
        * the $sort modifier to sort all the elements of the modified quizzes
        array by the score field in descending order, and
        * the $slice modifier to keep only the first three sorted elements of
        the quizzes array.
        """
        await self.collection.update_one(
            {'_id': self.doc['_id']},
            {
                "$push": {
                    "quizzes": {
                        "$each": [
                            {"wk": 5, "score": 8},
                            {"wk": 6, "score": 7},
                            {"wk": 7, "score": 6}
                        ],
                        "$sort": {"score": -1},
                        "$slice": 3
                    }
                }
            }
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "quizzes": [
                    {"wk": 1, "score": 10},
                    {"wk": 2, "score": 8},
                    {"wk": 5, "score": 8}
                ]
            }
        )

    async def test_push_operators_modifiers_gets_converted_into_set_operator(self):
        await self.collection.update(
            {'_id': self.doc['_id']},
            {
                "$push": {
                    "quizzes": {
                        "$each": [
                            {"wk": 5, "score": 8},
                            {"wk": 6, "score": 7},
                            {"wk": 7, "score": 6}
                        ],
                        "$sort": {"score": -1},
                        "$slice": 3
                    }
                }
            }
        )

        with asynctest.patch.object(self.observer.operation_handler,
                                    'handle') as handle:
            await self.observer.observe_changes()
            operation = handle.call_args_list[1][1]['operation']
            self.assertIn("$set", operation['o'])
