import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class MaxOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                       asynctest.TestCase):
    """
    $max

    The $max operator updates the value of the field to a specified value if
    the specified value is greater than the current value of the field. The
    $max operator can compare values of different types, using the BSON
    comparison order.

    Behavior

    If the field does not exists, the $max operator sets the field to the
    specified value.
    """
    template_doc = {"highScore": 800, "lowScore": 200}

    async def test_it_updates_values_lower_than_max(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$max": {
                "highScore": 950
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 950, "lowScore": 200}
        )

    async def test_it_does_nothing_to_values_higher_than_max(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$max": {
                "highScore": 700
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 800, "lowScore": 200}
        )

    async def test_an_update_operation_with_max_lower_than_value_doesnt_result_into_a_oplog_entry(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$max": {
                "highScore": 700
            }
        })

        with asynctest.patch.object(self.observer.operation_handler,
                                    'on_update') as on_update:
            await self.observer.observe_changes()
            on_update.assert_not_called()
