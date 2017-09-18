import abc
from typing import Dict

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection
from pathdict import PathDict

from mongo_observer.conf import logger


class Operations:
    INSERT = 'i'
    UPDATE = 'u'
    DELETE = 'd'
    COMMAND = 'c'
    DB_DECLARE = 'db'
    NO_OP = 'n'


class Document(PathDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, create_if_not_exists=True)


class OperationHandler(metaclass=abc.ABCMeta):
    def __init__(self):
        self.handlers = {
            Operations.INSERT: self.on_insert,
            Operations.UPDATE: self.on_update,
            Operations.DELETE: self.on_delete
        }

    async def handle(self, operation):
        try:
            handler = self.handlers[operation['op']]
        except KeyError:
            pass  # Unintersting operation
            print("skipping operation ", operation)
        else:
            await handler(operation)

    @abc.abstractmethod
    async def on_insert(self, operation):
        pass

    @abc.abstractmethod
    async def on_update(self, operation):
        pass

    @abc.abstractmethod
    async def on_delete(self, operation):
        pass


class LiveCollection(OperationHandler):
    def __init__(self,
                 collection: Dict[ObjectId, Dict],
                 remote_collection: AsyncIOMotorCollection):
        super().__init__()
        self.collection = collection
        self.remote_collection = remote_collection

    @classmethod
    async def init_async(cls, remote_collection: AsyncIOMotorCollection):
        cursor = remote_collection.find({})
        collection = {doc['_id']: Document(doc) async for doc in cursor}
        return cls(collection, remote_collection)

    async def on_update(self, operation):
        doc = self.collection[operation['o2']['_id']]
        change = operation['o']
        if '$set' in change:
            doc.update(change['$set'])
            logger.debug({'action': 'update', 'change': change['$set']})
        return doc

    async def on_insert(self, operation):
        doc = operation['o']
        self.collection[doc['_id']] = Document(doc)
        return doc

    async def on_delete(self, operation):
        doc = operation['o']
        del self.collection[doc['_id']]


class BuyboxChangeNotifier(LiveCollection):
    def get_buybox_change(self, previous_state, current_state):
        if before_update['customer']['is_buybox']:
            pass

    async def on_update(self, operation):
        # comparar o e o2
        before_update = self.collection[operation['o2']['_id']].copy()
        await super().on_update(operation)

        after_update = self.collection[operation['o2']['_id']]
        if before_update['customer']['is_buybox'] != after_update['customer']['is_buybox']:
            if after_update['customer']['is_buybox'] is True:
                print("Ganhou buybox")
            else:
                print("Perdeu buybox")