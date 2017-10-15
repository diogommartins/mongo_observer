import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class MulOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                       asynctest.TestCase):
    """
    $mul

    Multiply the value of a field by a number. The field to update must contain
    a numeric value.

    Missing Field

    If the field does not exist in a document, $mul creates the field and
    sets the value to zero of the same numeric type as the multiplier.

    Mixed Type

    Multiplication with values of mixed numeric types (32-bit integer,
    64-bit integer, float) may result in conversion of numeric type. For
    multiplication with values of mixed numeric types, the following type
    conversion rules apply:
    """
    template_doc = {"dog": "Xablau", "price": 10.99}

    async def test_it_multiply_the_value_of_a_field(self):
        await self.observer.observe_changes()
        self.assertEqual(self.handler.collection,
                         {self.doc['_id']: self.doc})

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$mul": {
                "price": 1.25
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "dog": "Xablau", "price": 13.7375}
        )

    async def test_using_mul_to_non_existing_field_created_it_with_zero_value(self):
        await self.collection.update_one({'_id': self.doc['_id']},
                                         {"$unset": {"price": ""}})
        await self.observer.observe_changes()
        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "dog": "Xablau"}
        )

        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$mul": {
                "price": 1.25
            }
        })

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {"_id": self.doc['_id'], "dog": "Xablau", "price": 0}
        )
