import abc
from typing import Dict

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection

from mongo_observer.conf import logger
from mongo_observer.models import Operations, Document


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
            print("skipping operation ", operation) # todo: remove debug
        else:
            await handler(operation)

    @abc.abstractmethod
    async def on_insert(self, operation):
        """
        :param operation: A dict containing a document corresponding
        to an operation on oplog. It will contain the following keys:

            * `ts`: Timestamp of the operation
            * `h`: An unique signed long identifier of the operation
            * `op`: A character representing the type of the operation
            * `ns`: A namespace string formed with the concatenation
            of 'database.collection'
            * `o`: The inserted document
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_update(self, operation):
        """
        :param operation: A dict containing a document corresponding
        to an operation on oplog. It will contain the following keys:

            * `ts`: Timestamp of the operation
            * `h`: An unique signed long identifier of the operation
            * `op`: A character representing the type of the operation
            * `ns`: A namespace string formed with the concatenation
            of 'database.collection'
            * `o`: The operation data performed on the document
            * `o2`: A dict with a single _id key of the document to be updated
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_delete(self, operation):
        """
        :param operation: A dict containing a document corresponding
        to an operation on oplog. It will contain the following keys:

            * `ts`: Timestamp of the operation
            * `h`: An unique signed long identifier of the operation
            * `op`: A character representing the type of the operation
            * `ns`: A namespace string formed with the concatenation
            of 'database.collection'
            * `o`: The operation data performed on the document
            * `o2`: A dict with a single _id key, of the deleted document
        """
        raise NotImplementedError()


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
        if previous_state['customer']['is_buybox']:
            pass

    async def on_update(self, operation):
        # comparar o e o2
        await super().on_update(operation)
        after_update = self.collection[operation['o2']['_id']]
        if after_update['customer']['is_buybox'] is True:
            print("Ganhou buybox")
        else:
            print("Perdeu buybox")
