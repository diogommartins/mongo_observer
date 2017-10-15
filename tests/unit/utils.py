class AsyncIterMockCursor:
    alive = True

    def __init__(self, seq):
        self.iter = iter(seq)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration
