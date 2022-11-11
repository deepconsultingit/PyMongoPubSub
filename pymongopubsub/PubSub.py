import abc
import threading
import time
import logging
from contextlib import contextmanager
from threading import Event
from typing import Callable

import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.database import Database, Collection
from bson.objectid import ObjectId

from dataclasses import dataclass, field, asdict

from pymongopubsub.Utils import logConfiguration


class PubSubError(Exception):
    pass


@dataclass
class AbstractPubSubMessage(abc.ABC):
    def toDict(self):
        return asdict(self)


@dataclass
class PubSubSubscription(AbstractPubSubMessage):
    QUEUE_NAME: str
    COLLECTION_NAME: str

    def __post_init__(self):
        super(AbstractPubSubMessage, self).__init__()
        pass


@dataclass
class PubSubMessage(AbstractPubSubMessage):
    QUEUE_NAME: str
    TOPIC: str
    PAYLOAD: dict
    CLIENT_ID: str = field(default=None)
    TS: int = field(default_factory=lambda: int(time.time() * 1000))

    def __post_init__(self):
        super(AbstractPubSubMessage, self).__init__()
        pass


class PubSubConnector(abc.ABC):
    PUBSUB_CAPPED_MAXOBJECTS_COLLECTION = 20000
    PUBSUB_CAPPED_SIZE_COLLECTION = 5 * 1024 * 1024  # 5 MiBi
    PUBSUB_REGISTRY_COLLECTION = "REGISTRY"
    PUBSUB_PUBLISH_COLLECTION = "PUBLISH"
    PUBSUB_DEFAULT_DB = "PSDB"

    PUBSUB_CLOSE_TOPIC = "__CLOSE__"
    PUBSUB_HEARTBEAT_TOPIC = "__HEARTBEAT__"
    PUBSUB_INACTIVE_CLIENT_TIMEOUT = 30.0

    @classmethod
    def createPubSubMongoClient(cls, connectionURI: str) -> MongoClient:
        return MongoClient(connectionURI)

    def __init__(self, client: MongoClient, databaseName: str):
        logConfiguration()
        self.log = logging.getLogger(f"{self.__class__.__name__}_{str(int(time.time() * 1000))}")

        self.__client = client
        self.__databaseName = databaseName
        self.__validatePublishCollection()
        self.__validatePublishCollectionStopper()
        pass

    def __validatePublishCollection(self):
        self.log.debug(f"Validate publish collection. DATABASE='{self.pubSubDatabase.name}' COLLECTION='{PubSubConnector.PUBSUB_PUBLISH_COLLECTION}'")
        isDefined = False
        cl = self.pubSubDatabase.list_collections()
        for c in cl:
            if c['name'] == PubSubConnector.PUBSUB_PUBLISH_COLLECTION:
                isDefined = True
                break

        if not isDefined:
            self.log.debug(f"Create capped collection for PUBSUB info. COLLECTION: '{PubSubConnector.PUBSUB_PUBLISH_COLLECTION}'")
            self.pubSubDatabase.create_collection(
                PubSubConnector.PUBSUB_PUBLISH_COLLECTION,
                capped=True,
                size=PubSubConnector.PUBSUB_CAPPED_SIZE_COLLECTION,
                max=PubSubConnector.PUBSUB_CAPPED_MAXOBJECTS_COLLECTION
            )
        pass

    def __validatePublishCollectionStopper(self):
        self.log.debug(f"Validate publish collection content. COLLECTION='{PubSubConnector.PUBSUB_PUBLISH_COLLECTION}'")
        first = self.publishCollection.find().sort(
            '$natural',
            pymongo.ASCENDING
        ).limit(-1)

        if len(list(first)) == 0:
            self.log.debug(f"Create stopper record in {PubSubConnector.PUBSUB_PUBLISH_COLLECTION} collection")
            self.pubSubDatabase[PubSubConnector.PUBSUB_PUBLISH_COLLECTION].insert_one(
                {
                    "TS": int(time.time() * 1000)
                }
            )

    @property
    def mongoClient(self) -> MongoClient:
        return self.__client

    @property
    def pubSubDatabase(self) -> Database:
        return self.mongoClient[self.__databaseName]

    @property
    def publishCollection(self) -> Collection:
        self.__validatePublishCollection()
        return self.pubSubDatabase[PubSubConnector.PUBSUB_PUBLISH_COLLECTION]

    @property
    def pubSubRegistryCollection(self) -> Collection:
        return self.pubSubDatabase[PubSubConnector.PUBSUB_REGISTRY_COLLECTION]

    pass


class Server(PubSubConnector):
    def __init__(self, client: MongoClient, databaseName: str = PubSubConnector.PUBSUB_DEFAULT_DB):
        super().__init__(client, databaseName)

        self.__isRunning = True
        self.__internalThread = threading.Thread(target=self.__run)
        self.__internalThread.daemon = True

        self.__heartbeatThread = threading.Thread(target=self.__heartbeat)
        self.__heartbeatThread.daemon = True

        self.__serviceThread = threading.Thread(target=self.__service)
        self.__serviceThread.daemon = True

        self.__activeClients = dict()

        self._closeEvent = Event()

        self.log.info("Server started.")
        pass

    def __removeRegistryById(self, clientId: ObjectId):
        # Remove the subscription in the control collection
        self.pubSubRegistryCollection.delete_one(
            {
                '$and': [
                    {"_id": clientId}
                ]
            }
        )
        pass

    def __dropCollectionByName(self, collectionName: str):
        if collectionName is None or collectionName not in list(self.pubSubDatabase.list_collection_names()):
            return
        # Drop the client collection
        self.pubSubDatabase.drop_collection(collectionName)
        pass

    def __cleanClient(self, clientId: ObjectId, queueName: str):
        if clientId is None:
            return
        if queueName is None or queueName == '':
            return

        collectionName = queueName + "_" + str(clientId).lower()

        self.log.debug(f"Clean client registration. CLIENT='{clientId}'")
        try:
            self.__removeRegistryById(clientId)
            self.__dropCollectionByName(collectionName)
        except PyMongoError as e:
            self.log.error(e)
            raise e
        pass

    def __service(self):
        t0 = int(time.time() * 1000)
        while self.__isRunning:
            self.log.debug(f"Check client status")

            # print(self.__activeClients)

            collections = list(self.pubSubDatabase.list_collection_names(filter={
                "name": {
                    "$nin": [
                        PubSubConnector.PUBSUB_REGISTRY_COLLECTION,
                        PubSubConnector.PUBSUB_PUBLISH_COLLECTION
                    ]
                }
            }))

            registrations = [r['_id'] for r in list(self.pubSubRegistryCollection.find({}))]

            # print(registrations)
            # print(collections)

            now = int(time.time() * 1000)
            clkl = list(self.__activeClients.keys())
            for clk in clkl:
                (cl, qu) = clk
                ts = self.__activeClients[clk]
                collectionName = f"{qu}_{str(cl).lower()}"
                self.log.debug(f"Check client {cl} and queue {qu}")
                if (now - ts) > (PubSubConnector.PUBSUB_INACTIVE_CLIENT_TIMEOUT * 1000):
                    self.log.warning(f"Client {cl} referred to queue {qu} is unavailable. Purge referencies.")
                    self.__cleanClient(cl, qu)
                    del self.__activeClients[(cl, qu)]
                else:
                    self.log.debug(f"Client {cl} is alive.")

                # Check if current client/queue belongs to list of all collections. If so it is removed from list.
                # List will contain only collections name referred to unactive/dead clients.
                if collectionName in collections:
                    collections.remove(collectionName)

                # Check if current client belongs to list of all subscribed clients. If so it is removed from list.
                # List will contain only clients name referred to unactive/dead clients.
                if cl in registrations:
                    registrations.remove(cl)

            if (now - t0) > (PubSubConnector.PUBSUB_INACTIVE_CLIENT_TIMEOUT * 1000) and len(self.__activeClients) > 0:
                if len(collections) > 0:
                    for cn in collections:
                        self.log.warning(f"Drop zombie collection. COLLECTION='{cn}'")
                        self.__dropCollectionByName(cn)
                if len(registrations) > 0:
                    for reg in registrations:
                        self.log.warning(f"Remove zombie registration. CLIENT='{str(reg)}'")
                        self.__removeRegistryById(reg)
            else:
                self.log.debug("Check client. No valid conditions to evaluate zombie clients.")

            time.sleep(10.0)
            pass
        pass

    def __heartbeat(self):
        first = self.publishCollection.find().sort(
            '$natural',
            pymongo.ASCENDING
        ).limit(-1).next()
        ts = first['TS']

        while self.__isRunning:
            self.log.debug("Open/Reopen cursor to read heartbeat messages...")
            cursor = self.publishCollection.find(
                {
                    "$and": [
                        {'TS': {'$gt': ts}},
                        {
                            'TOPIC': {
                                '$in': [
                                    PubSubConnector.PUBSUB_HEARTBEAT_TOPIC,
                                    PubSubConnector.PUBSUB_CLOSE_TOPIC
                                ]
                            }
                        }
                    ]
                },
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                oplog_replay=True
            )

            while cursor.alive:
                for doc in cursor:
                    message = PubSubMessage(
                        QUEUE_NAME=doc['QUEUE_NAME'],
                        TOPIC=doc['TOPIC'],
                        PAYLOAD=doc['PAYLOAD'],
                        CLIENT_ID=doc['CLIENT_ID'],
                        TS=doc['TS'],
                    )

                    if message.TOPIC == PubSubConnector.PUBSUB_HEARTBEAT_TOPIC:
                        self.log.debug(f"Received heartbeat message. Client='{message.CLIENT_ID}'")
                        self.__activeClients[(message.CLIENT_ID, message.QUEUE_NAME)] = message.TS

                    if message.TOPIC == PubSubConnector.PUBSUB_CLOSE_TOPIC:
                        self.log.debug(f"Received close message. Client='{message.CLIENT_ID}'")
                        self.__cleanClient(message.CLIENT_ID, message.QUEUE_NAME)

                    self.publishCollection.delete_one({"_id": doc["_id"]})
                time.sleep(0.025)
            # We end up here if the find() returned no documents or if the
            # tailable cursor timed out (no new documents were added to the
            # collection for more than 0.1 second).
            cursor.close()
            time.sleep(0.1)
            pass  # End while __isRunning
        pass  # End function

    def __run(self):
        first = self.publishCollection.find().sort(
            '$natural',
            pymongo.ASCENDING
        ).limit(-1).next()
        ts = first['TS']

        while self.__isRunning:
            self.log.debug("Open/Reopen cursor...")
            cursor = self.publishCollection.find(
                {
                    "$and": [
                        {'TS': {'$gt': ts}},
                        {
                            'TOPIC': {
                                '$nin': [
                                    PubSubConnector.PUBSUB_HEARTBEAT_TOPIC,
                                    PubSubConnector.PUBSUB_CLOSE_TOPIC
                                ]
                            }
                        }
                    ]
                },
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                oplog_replay=True
            )

            while cursor.alive:
                for doc in cursor:
                    message = PubSubMessage(
                        QUEUE_NAME=doc['QUEUE_NAME'],
                        TOPIC=doc['TOPIC'],
                        PAYLOAD=doc['PAYLOAD'],
                        CLIENT_ID=doc['CLIENT_ID'],
                        TS=doc['TS'],
                    )

                    subC = self.pubSubRegistryCollection.find({
                        '$and': [
                            {'QUEUE_NAME': message.QUEUE_NAME},
                            {'_id': {'$ne': message.CLIENT_ID}}
                        ]
                    })

                    for sub in subC:
                        collName = sub['COLLECTION_NAME']
                        self.pubSubDatabase[collName].insert_one(message.toDict())
                        self.log.debug(f"Dispatched to {sub['_id']}. Message -> {str(message)} from client '{message.CLIENT_ID}' and queue '{message.QUEUE_NAME}'")

                    self.publishCollection.delete_one({"_id": doc["_id"]})
                time.sleep(0.025)
            # We end up here if the find() returned no documents or if the
            # tailable cursor timed out (no new documents were added to the
            # collection for more than 0.1 second).
            cursor.close()
            time.sleep(0.1)
            pass  # End while __isRunning
        pass  # End function

    def start(self):
        self.log.debug(f"Start heartbeat thread")
        self.__heartbeatThread.start()
        self.log.debug(f"Start service thread")
        self.__serviceThread.start()
        self.log.debug(f"Start internal thread")
        self.__internalThread.start()
        pass

        self._closeEvent.wait()

    def close(self):
        self._closeEvent.set()


class Client(PubSubConnector):
    def __init__(self, client: MongoClient, databaseName: str = PubSubConnector.PUBSUB_DEFAULT_DB):
        super().__init__(client, databaseName)
        self.__callback = None
        self.__queueName = None
        self.__id = ObjectId()

        self.__isSubscribed = False
        self.__isRunning = False
        self.__internalThread = threading.Thread(target=self.__run)
        self.__internalThread.daemon = True

        self.__heartbeatThread = threading.Thread(target=self.__heartbeat)
        self.__internalThread.daemon = True

    @property
    def clientId(self):
        return self.__id

    @property
    def queueName(self):
        return self.__queueName

    @property
    def collectionName(self):
        return self.queueName + "_" + str(self.clientId).lower()

    def close(self):
        if not self.__isSubscribed:
            self.log.debug(f"Client doesn't has connection and subscriptions. CLIENT_ID='{str(self.__id).lower()}'")
            return

        self.log.debug(f"Close client connection. CLIENT_ID='{str(self.__id).lower()}'")
        self.__isRunning = False
        try:
            self.log.debug(f"Close message. CLIENT='{str(self.clientId).lower()}'")
            closeMessage = PubSubMessage(
                QUEUE_NAME=self.queueName,
                TOPIC=PubSubConnector.PUBSUB_CLOSE_TOPIC,
                PAYLOAD={},
                CLIENT_ID=self.clientId,
                TS=int(time.time() * 1000),
            )
            self.notify(closeMessage)

            # Reset callback function
            self.__callback = None

            # Wait 1 second to close thread
            self.__internalThread.join(1.0)
        except Exception as e:
            self.log.error(e)
            raise e

    def subscribe(self, queue: str, _callback: Callable[[PubSubMessage], None]):
        if queue is None or queue == "":
            return

        if _callback is None or not callable(_callback):
            return

        self.__queueName = queue.strip().replace(' ', '')   # This entry is fundamental to enable the
                                                            # property "collectionName" that is based on a
                                                            # valid value (not None) of self.__queueName
        self.log.debug(f"Create capped collection. COLLECTION='{self.collectionName}' QUEUE='{self.queueName}'")
        self.pubSubDatabase.create_collection(
            self.collectionName,
            capped=True,
            size=PubSubConnector.PUBSUB_CAPPED_SIZE_COLLECTION,
            max=PubSubConnector.PUBSUB_CAPPED_MAXOBJECTS_COLLECTION
        )

        self.pubSubDatabase[self.collectionName].insert_one(
            {
                "TS": int(time.time() * 1000)
            }
        )

        self.log.debug(f"Create entry in PUBSUB_SERVER_CONTROL_COLLECTION. COLLECTION='{self.pubSubRegistryCollection.name}' CLIENT_ID='{str(self.clientId).lower()}' QUEUE_NAME='{self.queueName}' REFERENCED_COLLECTION='{self.collectionName}'")
        subscription = PubSubSubscription(
            QUEUE_NAME=self.queueName,
            COLLECTION_NAME=self.collectionName
        )
        doc = subscription.toDict()
        doc['_id'] = self.clientId

        self.pubSubRegistryCollection.insert_one(doc)

        self.__isSubscribed = True
        self.__isRunning = True

        self.__callback = _callback
        self.log.debug(f"Start internal thread. CLIENT='{str(self.clientId).lower()}'")
        self.__internalThread.start()
        self.log.debug(f"Start heartbeat thread. CLIENT='{str(self.clientId).lower()}'")
        self.__heartbeatThread.start()
        self.log.debug(f"Client {str(self.clientId).lower()} subscribed to queue ´{self.queueName}´")

    def notify(self, message: PubSubMessage):
        if message.TS is None or message.TS == 0 or message.TS == -1:
            message.TS = int(time.time() * 1000)
        message.CLIENT_ID = self.clientId
        try:
            doc = message.toDict()
            self.publishCollection.insert_one(doc)
        except PyMongoError as e:
            raise PubSubError(str(e))

    def __run(self):
        if self.__callback is None:
            return

        first = self.pubSubDatabase[self.collectionName].find().sort(
            '$natural',
            pymongo.ASCENDING
        ).limit(-1).next()
        ts = first['TS']

        while self.__isRunning:
            cursor = self.pubSubDatabase[self.collectionName].find(
                {
                    'TS': {'$gt': ts}
                },
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                oplog_replay=True
            )
            while cursor.alive:
                for doc in cursor:
                    message = PubSubMessage(
                        QUEUE_NAME=doc['QUEUE_NAME'],
                        TOPIC=doc['TOPIC'],
                        PAYLOAD=doc['PAYLOAD'],
                        CLIENT_ID=doc['CLIENT_ID'],
                        TS=doc['TS'],
                    )

                    if callable(self.__callback):
                        self.__callback(message)
                    self.pubSubDatabase[self.collectionName].delete_one({"_id": doc["_id"]})
                time.sleep(0.025)
                # We end up here if the find() returned no documents or if the
                # tailable cursor timed out (no new documents were added to the
                # collection for more than 0.1 second).
            cursor.close()
            time.sleep(0.1)
            pass  # End while __isRunning
        pass  # End function

    def __heartbeat(self):
        while self.__isRunning:
            self.log.debug(f"Heartbeat message. CLIENT='{str(self.clientId).lower()}'")
            heartbeatMessage = PubSubMessage(
                QUEUE_NAME=self.queueName,
                TOPIC=PubSubConnector.PUBSUB_HEARTBEAT_TOPIC,
                PAYLOAD={},
                CLIENT_ID=self.clientId,
                TS=int(time.time() * 1000),
            )
            self.notify(heartbeatMessage)

            time.sleep(10.0)
            pass

    @contextmanager
    def context(self):
        try:
            self.log.debug(f"Open client context. CLIENT='{self.clientId}'")
            yield self
        finally:
            self.log.debug(f"Close client context. CLIENT='{self.clientId}'")
            self.close()
        pass
    pass

