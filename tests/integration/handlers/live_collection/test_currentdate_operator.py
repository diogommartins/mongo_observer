import asynctest
from unittest.mock import ANY
from datetime import datetime

from bson import Timestamp

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class CurrentDateOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                                asynctest.TestCase):
    """
    $currentDate

    The $currentDate operator sets the value of a field to the current date,
    either as a Date or a timestamp. The default type is Date.

    Behavior

    If the field does not exist, $currentDate adds the field to a document.

    """
    template_doc = {
        "status": "a",
        "modified_at": Timestamp(datetime(2006, 6, 6, 6, 6, 6), 0)
    }

    async def test_it_updates_existing_date_field(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$currentDate": {
                "modified_at": True,
            },
            "$set": {
                "status": "D",
                "cancellation.reason": "user request"
            }
        })

        await self.observer.observe_changes()
        self.assertEqual(
            self.handler.collection[self.doc['_id']],
            {
                '_id': self.doc['_id'],
                'status': 'D',
                'modified_at': ANY,
                'cancellation': {
                    'reason': 'user request'
                }
            }
        )
        affected_doc = self.handler.collection[self.doc['_id']]
        self.assertIsInstance(affected_doc['modified_at'], datetime)
        self.assertEqual(
            affected_doc['modified_at'].date(),
            datetime.today().date()
        )

    async def test_it_creates_new_datetime_fields(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$currentDate": {
                "cancellation.date": {"$type": "date"}
            },
            "$set": {
                "status": "D",
                "cancellation.reason": "user request"
            }
        })

        await self.observer.observe_changes()
        affected_doc = self.handler.collection[self.doc['_id']]
        self.assertIsInstance(affected_doc['cancellation']['date'], datetime)
        self.assertEqual(
            affected_doc['cancellation']['date'].date(),
            datetime.today().date()
        )

    async def test_it_creates_new_timestamp_fields(self):
        await self.collection.update_one({'_id': self.doc['_id']}, {
            "$currentDate": {
                "cancellation.date": {"$type": "timestamp"}
            },
            "$set": {
                "status": "D",
                "cancellation.reason": "user request"
            }
        })

        await self.observer.observe_changes()
        affected_doc = self.handler.collection[self.doc['_id']]
        self.assertIsInstance(affected_doc['cancellation']['date'], Timestamp)
        self.assertEqual(
            affected_doc['cancellation']['date'].as_datetime().date(),
            datetime.today().date()
        )
