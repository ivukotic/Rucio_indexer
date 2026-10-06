import time
import uuid
import json
import logging
import threading
import stomp

log = logging.getLogger(__name__)

_RECONNECT_DELAY_MAX = 60  # seconds


class ActiveMqListener(stomp.ConnectionListener):
    """
    ActiveMQ client
    """

    def __init__(self, host, port, topic, callback, user, password):
        """
        Constructor
        """
        self.id = str(uuid.uuid4())
        self.user = user
        self.password = password
        self._reconnect_lock = threading.Lock()
        # heartbeats=(send_ms, recv_ms) — broker must reply within recv_ms
        # or the stomp library fires on_heartbeat_timeout automatically.
        # reconnect_attempts_max is the number of socket-open attempts made
        # inside a single connect() call (NOT a background reconnect thread).
        # 0 means the socket is never opened and connect() always raises an
        # empty ConnectFailedException. Use 1 and let _reconnect() below own
        # retries/backoff.
        self.connection = stomp.Connection(
            [(host, port)], heartbeats=(4000, 4000), reconnect_attempts_max=1
        )
        self.connection.set_listener('MessagingListener', self)
        self.topic = topic
        self.callback = callback
        # Handles initial connect with backoff so startup failures don't
        # crash the process.
        self._reconnect()

    def on_connecting(self, host_and_port):
        log.debug(f'ActiveMQ connected socket to {str(host_and_port)}')

    def on_connected(self, frame):
        log.info(f'ActiveMQ connected {frame.body}')
        self.connection.subscribe(
            destination=self.topic,
            id=f'activemq-listener-{self.id}',
            ack='auto',
            headers={"durable": True, "auto-delete": False}
        )

    def on_disconnected(self):
        log.warning('ActiveMQ connection lost. Reconnecting with backoff...')
        self._reconnect()

    def on_heartbeat_timeout(self):
        # stomp.py already disconnects the socket when a heartbeat times out,
        # which fires on_disconnected — that handler owns reconnection.
        # Calling disconnect() here races with an already-completed reconnect
        # and sends a duplicate CONNECT frame, which the broker rejects with
        # "duplicate CONNECT or STOMP frame".
        log.warning(
            'ActiveMQ heartbeat timeout '
            '(reconnect handled by on_disconnected).'
        )

    def _reconnect(self):
        # on_disconnected() and on_heartbeat_timeout() can both fire around
        # the same time; without this guard they would call connect()
        # concurrently and send overlapping CONNECT frames on the same
        # socket, which the broker rejects as a duplicate CONNECT.
        if not self._reconnect_lock.acquire(blocking=False):
            log.debug('Reconnect already in progress, skipping.')
            return
        try:
            delay = 5
            attempt = 0
            while True:
                attempt += 1
                try:
                    log.info(f'Reconnect attempt {attempt}...')
                    self.connection.connect(
                        self.user, self.password, wait=True
                    )
                    return
                except Exception as e:
                    log.warning(
                        f'Reconnect attempt {attempt} failed: '
                        f'({type(e).__name__}) {e}. '
                        f'Retry in {delay}s...'
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, _RECONNECT_DELAY_MAX)
        finally:
            self._reconnect_lock.release()

    def on_message(self, frame):
        message = frame.body[:-1]  # Skip EOT
        try:
            content = json.loads(message)
            self.callback(content)
        except Exception as e:
            log.warning(
                'Failed to process message: (%s) %s' % (type(e).__name__, e)
            )
            log.warning(message)
            raise

    def on_error(self, frame):
        log.error(f'ActiveMQ error: {frame.body}')
