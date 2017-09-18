from pymongo import DESCENDING, CursorType
from asynctest import TestCase, CoroutineMock, Mock, patch, call

from mongo_observer.observer import Observer, ShouldStopObservation


class ObserverTests(TestCase):
    async def test_filter_is_generated_correctly_with_namespace_filter(self):
        ns = Mock()
        ts = Mock()
        # noinspection PyTypeChecker
        observer = await Observer.init_async(
            oplog=Mock(),
            operation_handler=Mock(),
            namespace_filter=ns,
            starting_timestamp=ts)
        self.assertEqual(observer.filter, {'ns': ns, 'ts': {'$gt': ts}})

    async def test_filter_is_generated_correctly_without_namespace_filter(self):
        ts = Mock()
        # noinspection PyTypeChecker
        observer = await Observer.init_async(
            oplog=Mock(),
            operation_handler=Mock(),
            starting_timestamp=ts)
        self.assertEqual(observer.filter, {'ts': {'$gt': ts}})

    async def test_get_new_cursor_return_a_new_tailable_cursor(self):
        oplog = Mock()
        observer = await Observer.init_async(oplog, Mock(), Mock(), Mock())
        cursor = observer.get_new_cursor()

        self.assertEqual(cursor, oplog.find.return_value)
        oplog.find.assert_called_once_with(observer.filter,
                                           cursor_type=CursorType.TAILABLE_AWAIT)

    async def test_init_async_gets_last_document_ts_if_starting_ts_isnt_provided(self):
        oplog = CoroutineMock()
        timestamp = Mock()
        oplog.find_one.return_value = {'ts': timestamp}
        observer = await Observer.init_async(oplog, Mock(), Mock())

        oplog.find_one.assert_called_once_with(sort=[('$natural', DESCENDING)])
        self.assertEqual(observer.filter['ts'], {'$gt': timestamp})


class ObserverObserveChangesTests(TestCase):
    async def setUp(self):
        self.oplog = CoroutineMock()
        self.handler = CoroutineMock()

        # noinspection PyTypeChecker
        self.observer = await Observer.init_async(
            oplog=self.oplog,
            operation_handler=self.handler,
            starting_timestamp=Mock(),
            on_nothing_to_fetch_on_cursor=self.stop_infinite_iteration
        )

    async def stop_infinite_iteration(self):
        raise ShouldStopObservation()

    async def test_it_calls_handler_if_theres_a_document_to_fetch_on_cursor(self):
        cursor = CoroutineMock(next_object=Mock())
        cursor.fetch_next = CoroutineMock(return_value=True)()
        document = Mock()
        cursor.next_object.return_value = document

        with patch.object(self.observer, 'get_new_cursor', return_value=cursor):
            with self.assertRaises(ShouldStopObservation):
                await self.observer.observe_changes()

            self.handler.handle.assert_called_once_with(operation=document)

    async def test_it_doesnt_call_handler_if_theres_nothing_to_fetch_on_cursor(self):
        cursor = CoroutineMock(next_object=Mock())
        cursor.fetch_next = CoroutineMock(return_value=False)()
        document = Mock()
        cursor.next_object.return_value = document

        with patch.object(self.observer, 'get_new_cursor', return_value=cursor):
            with self.assertRaises(ShouldStopObservation):
                await self.observer.observe_changes()

            self.handler.handle.assert_not_called()

    async def test_it_gets_a_new_cursor_if_current_cursor_dies(self):
        dead_cursor = CoroutineMock(alive=False)

        with patch.object(self.observer, 'get_new_cursor',
                          side_effect=[dead_cursor, ShouldStopObservation]):
            with self.assertRaises(ShouldStopObservation):
                await self.observer.observe_changes()

            self.assertEqual(self.observer.get_new_cursor.call_args_list,
                             [call(), call()])

