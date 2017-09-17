import abc

from pathdict import PathDict


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
