import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class UnsetOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                          asynctest.TestCase):
    """
    $unset

    The $unset operator deletes a particular field.

    Behavior

    If the field does not exist, then $unset does nothing (i.e. no operation).
    When used with $ to match an array element, $unset replaces the matching
    element with null rather than removing the matching element from the array.
    This behavior keeps consistent the array size and element positions.
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

    async def test_it_removes_root_level_fields(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$unset": {
                "dog": "",
                "quantity": ""
            }
        })

        await self.observer.observe_changes()
        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
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
        )

    async def test_it_removes_embeded_document_fields(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$unset": {
                "details.make": ""
            }
        })

        await self.observer.observe_changes()
        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "quantity": 500,
                "dog": "Xablau",
                "details": {
                    "model": "14Q3"
                },
                "tags": ["coats", "outerwear", "clothing"],
                "ratings": [
                    {"name": "foo", "rating": 0},
                    {"name": "eggs", "rating": 4}
                ]
            }
        )

    async def test_trying_to_remove_an_unexisting_field_results_in_noop(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$unset": {
                "loan.me.a.dime": "",
                "dave.meniketti": "",
                "cat": ""
            }
        })

        with asynctest.patch.object(self.observer.operation_handler,
                                    'on_update') as on_update:
            await self.observer.observe_changes()
            on_update.assert_not_called()

    async def test_removing_an_array_field_updates_its_position_value_to_None(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$unset": {
                "ratings.0": ""
            }
        })

        await self.observer.observe_changes()
        self.assertIsNone(self.handler.collection[self.doc['_id']]['ratings'][0])
