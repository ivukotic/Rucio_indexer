#!/usr/bin/env python

import queue
import socket
import time
import threading
from threading import Thread
import copy
from datetime import datetime, timezone

import tools
from AMQ_Listener import ActiveMqListener

# virtual queue -> index prefix
queues = {
    "/queue/Consumer.ftsucanalytics.rucio.nongrid_tracer": "rucio-nongrid-traces",
    "/queue/Consumer.ftsucanalytics.rucio.tracer": "rucio-traces",
    "/queue/Consumer.ftsucanalytics.rucio.events": "rucio-events",
}

MQ_parameters = tools.get_MQ_connection_parameters()

q = queue.Queue(maxsize=5000)
_last_message_time = time.time()
_listeners = []


def make_inserter(index_prefix):
    def inserter(message):
        global _last_message_time
        _last_message_time = time.time()
        q.put((index_prefix, message))
    return inserter


print("starting ...")
PORT = 61013  # plain STOMP on atlas-mb brokers (61023 is STOMP+TLS)

ips = set()
for a in socket.getaddrinfo(MQ_parameters['MQ_HOST'], PORT):
    ips.add(a[4][0])

for ip in ips:
    if ip.count(':') > 0:
        continue
    for queue_name, index_prefix in queues.items():
        _listeners.append(
            ActiveMqListener(ip, PORT, queue_name, make_inserter(index_prefix),
                             MQ_parameters['MQ_USER'], MQ_parameters['MQ_PASS'])
        )


def get_timestamp(index_prefix, m):
    """ returns message time as aware datetime, falls back to now """
    try:
        if index_prefix == 'rucio-events':
            # e.g. "2026-10-06 12:34:56.123456"
            return datetime.fromisoformat(m['created_at']).replace(tzinfo=timezone.utc)
        if index_prefix == 'rucio-nongrid-traces':
            return datetime.fromtimestamp(float(m['timeentry']), timezone.utc)
        # tracer messages carry unix seconds
        return datetime.fromtimestamp(float(m['traceTimeentryUnix']), timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def eventCreator():
    aLotOfData = []
    es_conn = tools.get_es_connection()
    while True:
        index_prefix, m = q.get()
        try:
            if not isinstance(m, dict):
                continue
            dati = get_timestamp(index_prefix, m)
            data = copy.copy(m)
            data['_index'] = f'{index_prefix}-{dati.year}.{str(dati.month).zfill(2)}.{str(dati.day).zfill(2)}'
            data['@timestamp'] = int(dati.timestamp() * 1000)
            aLotOfData.append(data)
        except Exception as e:
            print('failed to process message:', type(e).__name__, e, m)
        finally:
            q.task_done()

        if len(aLotOfData) > 500:
            tools.bulk_index(aLotOfData, es_conn=es_conn,
                             thread_name=threading.current_thread().name)
            aLotOfData = []


# start eventCreator threads
for i in range(1):
    t = Thread(target=eventCreator)
    t.daemon = True
    t.start()


_SILENCE_THRESHOLD = 600  # seconds — treat as stale if no message received for 10 min

while True:
    time.sleep(55)
    now = datetime.now()
    silent_for = time.time() - _last_message_time
    print(now.strftime("%Y-%m-%d %H:%M:%S"), "qsize:",
          q.qsize(), f"| last msg: {int(silent_for)}s ago")
    if silent_for > _SILENCE_THRESHOLD:
        print(now.strftime("%Y-%m-%d %H:%M:%S"), "WARNING: no messages for",
              int(silent_for), "s — forcing reconnect")
        for listener in _listeners:
            try:
                listener.connection.disconnect()
            except Exception:
                pass
