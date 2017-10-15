import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class SetOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                        asynctest.TestCase):
    """
    $set

    The $set operator replaces the value of a field with the specified value.

    Behavior

    If the field does not exist, $set will add a new field with the specified
    value, provided that the new field does not violate a type constraint. If
    you specify a dotted path for a non-existent field, $set will create the
    embedded documents as needed to fulfill the dotted path to the field.

    If you specify multiple field-value pairs, $set will update or
    create each field.
    """

    template_doc = {
        "quantity": 500,
        "dog": "Xablau",
        "details": {
            "model": "14Q3",
            "make": "xyz"
        },
        "tags": ["coats", "outerwear", "clothing"],
        "ratings": [
            {"name": "foo", "rating": 0},
            {"name": "eggs", "rating": 4}
        ]
    }

    async def test_it_updates_current_fields_value(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$set": {
                "dog": "Xena"
            }
        })

        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection[self.doc['_id']]['dog'],
                         "Xena")

    async def test_it_creates_if_not_exists(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$set": {
                "details.dog": "Xena"
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']]['details']['dog'],
            "Xena"
        )

    async def test_it_updates_embeded_documents(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$set": {
                "details.model": "C3PO"
            }
        })

        await self.observer.observe_changes()
        self.assertEqual(
            self.handler.collection[self.doc['_id']]['details']['model'],
            "C3PO"
        )

    async def test_it_set_elements_in_arrays(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$set": {
                "ratings.1.rating": 666
            }
        })

        await self.observer.observe_changes()
        self.assertEqual(
            self.handler.collection[self.doc['_id']]['ratings'][1]['rating'],
            666
        )
