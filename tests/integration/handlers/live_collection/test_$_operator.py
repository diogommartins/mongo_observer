import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class DollarOperatorTests(LiveCollectionFieldUpdateOperatorsTests,
                          asynctest.TestCase):
    """
    $

    The positional $ operator identifies an element in an array to update
    without explicitly specifying the position of the element in the array.
    To project, or return, an array element from a read operation, see the
    $ projection operator.
    """
    template_doc = {
        "grades": [
            {"grade": 80, "mean": 75, "std": 8},
            {"grade": 85, "mean": 90, "std": 5},
            {"grade": 90, "mean": 85, "std": 3}
        ]
    }

    async def test_it_updates_values_in_array(self):
        await self.collection.update_one(
            {"_id": self.doc['_id']},
            {"$set": {"grades": [80, 85, 90]}
         })

        await self.collection.update_one(
            {
                "_id": self.doc['_id'],
                "grades": 80
            },
            {
                "$set": {"grades.$": 82}
            }
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "grades": [82, 85, 90]
            }
         )

    async def test_it_updates_documents_in_array(self):
        await self.collection.update_one(
            {
                "_id": self.doc['_id'],
                "grades.grade": 85
            },
            {
                "$set": {"grades.$.std": 6}
            }
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "grades": [
                    {"grade": 80, "mean": 75, "std": 8},
                    {"grade": 85, "mean": 90, "std": 6},
                    {"grade": 90, "mean": 85, "std": 3}
                ]
            }
        )

    async def test_it_updates_embeded_documents(self):
        await self.collection.update_one(
            {
                "_id": self.doc['_id'],
                "grades": {
                    "$elemMatch": {
                        "grade": {"$lte": 90},
                        "mean": {"$gt": 80}}
                }
            },
            {
                "$set": {"grades.$.std": 6}
            }
        )

        await self.observer.observe_changes()

        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                "_id": self.doc['_id'],
                "grades": [
                    {"grade": 80, "mean": 75, "std": 8},
                    {"grade": 85, "mean": 90, "std": 6},
                    {"grade": 90, "mean": 85, "std": 3}
                ]
            }
        )
