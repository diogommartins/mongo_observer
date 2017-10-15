import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class MinOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                       asynctest.TestCase):
    """
    $min

    The $min updates the value of the field to a specified value if the
    specified value is less than the current value of the field. The $min
    operator can compare values of different types, using the BSON comparison
    order.

    Behavior

    If the field does not exists, the $min operator sets the field to the
    specified value.

    For comparisons between values of different types, such as a number and
    a null, $min uses the BSON comparison order.
    """
    template_doc = {"highScore": 800, "lowScore": 200}

    async def test_it_updates_values_higher_than_min(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$min": {
                "lowScore": 150
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 800, "lowScore": 150}
        )

    async def test_it_does_nothing_to_values_lower_than_min(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$min": {
                "lowScore": 250
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "highScore": 800, "lowScore": 200}
        )

    async def test_an_update_operation_with_min_higher_than_value_doesnt_result_into_a_oplog_entry(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$min": {
                "lowScore": 250
            }
        })

        with asynctest.patch.object(self.observer.operation_handler,
                                    'on_update') as on_update:
            await self.observer.observe_changes()
            on_update.assert_not_called()
