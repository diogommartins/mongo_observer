import asynctest

from mongo_observer.operation_handlers import OperationHandler


class OperationHandlerTests(asynctest.TestCase):
    def setUp(self):
        class FooHandler(OperationHandler):
            on_insert = asynctest.CoroutineMock()
            on_delete = asynctest.CoroutineMock()
            on_update = asynctest.CoroutineMock()

        self.handler = FooHandler()
        self.operation = {
            'op': None,
            'ts': 666,
            'o': {'dog': 'Xablau'},
            'o2': {'dog': 'Xena'}
        }

    async def test_it_calls_onupdate_if_operation_is_u(self):
        self.operation['op'] = 'u'
        await self.handler.handle(self.operation)

        self.handler.on_update.assert_called_once_with(self.operation)
        self.handler.on_insert.assert_not_called()
        self.handler.on_delete.assert_not_called()

    async def test_it_calls_oninsert_if_operation_is_i(self):
        self.operation['op'] = 'i'
        await self.handler.handle(self.operation)

        self.handler.on_update.assert_not_called()
        self.handler.on_insert.assert_called_once_with(self.operation)
        self.handler.on_delete.assert_not_called()

    async def test_it_calls_ondelete_if_operation_is_d(self):
        self.operation['op'] = 'd'
        await self.handler.handle(self.operation)

        self.handler.on_update.assert_not_called()
        self.handler.on_insert.assert_not_called()
        self.handler.on_delete.assert_called_once_with(self.operation)

    async def test_it_doesnt_call_handlers_for_uninteresting_operations(self):
        self.operation['op'] = 'Xablau'
        await self.handler.handle(self.operation)

        self.handler.on_update.assert_not_called()
        self.handler.on_insert.assert_not_called()
        self.handler.on_delete.assert_not_called()
