# Mongo Observer

Mongo Observer aims to provide an easy way to asynchronously subscribe to 
state change events on a given mongo collection.

# Installation

`pip install mongo_observer`

# Usage

## The Observer

The `Observer` handles state change observation on a given collection and 
dispatches events, delegating the responsibility to a handler.

## Handlers

 Handlers are 
objects that implement the `OperationHandler` protocol and implement the 
following abstract methods:

### async def on_insert(self, operation: Dict[str, Any])
 
Where `operation` is a dict containing a document corresponding to an 
operation on oplog. It will contain the following keys:

* `ts`: Timestamp of the operationa
* `h`: An unique signed long identifier of the operation
* `op`: A character representing the type of the operation
* `ns`: A namespace string formed with the concatenation of 'database.collection'
* `o`: The inserted document

### async def on_update(self, operation: Dict[str, Any])

Where `operation` is a dict containing a document corresponding to an 
operation on oplog. It will contain the following keys:

* `ts`: Timestamp of the operation
* `h`: An unique signed long identifier of the operation
* `op`: A character representing the type of the operation
* `ns`: A namespace string formed with the concatenation
of 'database.collection'
* `o`: The operation data performed on the document
* `o2`: A dict with a single _id key of the document to be updated

### async def on_delete(self, operation: Dict[str, Any])

Where `operation` is a dict containing a document corresponding to an 
operation on oplog. It will contain the following keys:

* `ts`: Timestamp of the operation
* `h`: An unique signed long identifier of the operation
* `op`: A character representing the type of the operation
* `ns`: A namespace string formed with the concatenation
of 'database.collection'
* `o`: The operation data performed on the document
* `o2`: A dict with a single _id key, of the deleted document


## ReactiveCollection

A `ReactiveCollection` is a read-only, in-memory, non-persistent replica of a 
remote mongo collection. It reacts to state changes caused by write operations 
(inserts, updates and deletes) on the remote collection. 

```python
import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

from mongo_observer.observer import Observer
from mongo_observer.operation_handlers import ReactiveCollection


async def run(loop):
    client = AsyncIOMotorClient('mongodb://127.0.0.1')

    collection_to_observe = client['your_db']['your_collection']
    reactive_collection = await ReactiveCollection.init_async(collection_to_observe)
    
    observer = await Observer.init_async(oplog=client['local']['oplog.rs'],
                                         operation_handler=reactive_collection,
                                         namespace_filter='your_db.your_collection')
    
    loop.create_task(observer.observe_changes())


loop = asyncio.get_event_loop()
loop.run_until_complete(run(loop))
loop.run_forever()
```

`reactive_collection` 

### ReactivePartialCollection