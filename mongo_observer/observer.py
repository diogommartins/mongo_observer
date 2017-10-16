import asyncio
from typing import Callable, Coroutine

from bson import Timestamp
from pymongo import CursorType, DESCENDING

from mongo_observer import conf
from mongo_observer.operation_handlers import OperationHandler


class ShouldStopObservation(KeyboardInterrupt):
    pass


class Observer:
    def __init__(self,
                 oplog,
                 operation_handler: OperationHandler,
                 namespace_filter: str,
                 starting_timestamp: float,
                 on_nothing_to_fetch_on_cursor: Callable[..., Coroutine]):
        """
        :param oplog: Operation log collection
        :param operation_handler: Delegate object responsible of handling
        operations
        :param namespace_filter: `database.collection` affected by the operation
        :param timestamp: `ts` attribute on document root representing the
        timestamp of the operation
        :param on_nothing_to_fetch_on_cursor: Coroutine awaited when cursor has
        no data to be fetched
        """
        if namespace_filter is None:
            filter = {}
        else:
            filter = {'ns': namespace_filter}
        self.namespace_filter = namespace_filter
        self.filter = filter
        self.filter['ts'] = {'$gt': starting_timestamp}

        self.operation_handler = operation_handler
        self.oplog = oplog

        self.logger = conf.logger
        self.sleep_time = conf.SLEEP_TIME_BEFORE_GET_NEW_CURSOR_IN_SECONDS
        self.on_nothing_to_fetch_on_cursor = on_nothing_to_fetch_on_cursor

    @classmethod
    async def init_async(cls,
                         oplog,
                         operation_handler: OperationHandler,
                         namespace_filter: str=None,
                         starting_timestamp: Timestamp=None,
                         on_nothing_to_fetch_on_cursor=None):

        if starting_timestamp is None:
            last_doc = await oplog.find_one(sort=[('$natural', DESCENDING)])
            try:
                starting_timestamp = last_doc['ts']
            except TypeError:
                starting_timestamp = Timestamp(0, 1)

        return cls(oplog,
                   operation_handler,
                   namespace_filter,
                   starting_timestamp,
                   on_nothing_to_fetch_on_cursor)

    def get_new_cursor(self):
        if self.operation_handler.last_timestamp:
            self.filter['ts']['$gt'] = self.operation_handler.last_timestamp

        return self.oplog.find(self.filter,
                               cursor_type=CursorType.TAILABLE_AWAIT)

    async def observe_changes(self):
        cursor = self.get_new_cursor()
        try:
            while True:
                if not cursor.alive:
                    asyncio.sleep(self.sleep_time)
                    cursor = self.get_new_cursor()

                empty_cursor = True
                async for doc in cursor:
                    empty_cursor = False
                    await self.operation_handler.handle(operation=doc)
                if empty_cursor and self.on_nothing_to_fetch_on_cursor:
                    await self.on_nothing_to_fetch_on_cursor()
        except ShouldStopObservation:
            self.logger.debug('Stopping observer')
            return
