import asynctest
import pymongo

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class AddToSetOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                            asynctest.TestCase):
    """
    $addToSet

    The $addToSet operator adds a value to an array unless the value is already
    present, in which case $addToSet does nothing to that array.

    Behavior

    * $addToSet only ensures that there are no duplicate items added to the set
    and does not affect existing duplicate elements. $addToSet does not
    guarantee a particular ordering of elements in the modified set.

    * If the field is absent in the document to update, $addToSet creates the
    array field with the specified value as its element.

    * If the field is not an array, the operation will fail.

    * If the value is an array, $addToSet appends the whole array as a single
    element.


    """
    template_doc = {
        "letters": ["a", "b"],
        "dog": "Xablau"
    }

    async def test_it_inserts_elements_to_existing_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$addToSet": {"letters": ["c", "d"]}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a", "b", ["c", "d"]],
                "dog": "Xablau"
            }
        )

    async def test_it_inserts_single_element_to_existing_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$addToSet": {"letters": "c"}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a", "b", "c"],
                "dog": "Xablau"
            }
        )

    async def test_it_doesnt_inserts_element_if_already_exists_on_array(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$addToSet": {"letters": "a"}}
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

    async def test_it_inserts_with_each_modifier(self):
        await self.collection.update(
            {"_id": self.doc['_id']},
            {"$addToSet": {"letters": {"$each": ["c", "d"]}}}
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "letters": ["a", "b", "c", "d"],
                "dog": "Xablau"
            }
        )

    async def test_it_fails_if_not_array_field(self):
        with self.assertRaises(pymongo.errors.WriteError):
            await self.collection.update(
                {"_id": self.doc['_id']},
                {"$addToSet": {"dog": {"$each": ["c", "d"]}}}
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
