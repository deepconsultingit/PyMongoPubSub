# PyMongoPubSub

A small library that implements Pub-Sub messagging pattern backed by a MongoDB database.


## Install

```shell
python3 setup.py install
```

## Example

Server

```python
from pymongopubsub.PubSub import Server
from pymongo import MongoClient

def main():
    client = MongoClient()
    Server(client).start()

if __name__ == '__main__':
    main()
```

Consumer

```python
from pymongopubsub.PubSub import Client, PubSubMessage
from pymongo import MongoClient
from threading import Event

def cb(message: PubSubMessage):
    print('received', message)

def main():
    with Client(MongoClient()).context() as client:
        client.subscribe("myqueue", cb)
        Event().wait()

if __name__ == '__main__':
    main()
```

Publisher

```python
from pymongopubsub.PubSub import Client, PubSubMessage
from pymongo import MongoClient

def main():
    with Client(MongoClient()).context() as client:
        client.notify(PubSubMessage(
            QUEUE_NAME="myqueue",
            TOPIC="mytopic",
            PAYLOAD={
                'value': 1
            }
        ))

if __name__ == '__main__':
    main()
```

NOTE: the PubSubMessage has 2 optional fields (CLIENT_ID and TS) that are initialized to None and UTS (Unix TimeStamp) respectively. The method "notify" will set these fields accordingly with the client info.