import asynctest

from tests.integration.handlers.live_collection.base import \
    LiveCollectionFieldUpdateOperatorsTests


class UnsetOperationTests(LiveCollectionFieldUpdateOperatorsTests,
                          asynctest.TestCase):
    pass