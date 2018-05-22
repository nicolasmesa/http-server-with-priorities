import socket
import queue
import threading
import logging
import collections
import time

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


Request = collections.namedtuple('Request', [
    'method',
    'path',
    'http_version',
    'sock',
])

class HttpServerWithPriorities:
    """HTTP Server that has different priorities based on the request"""

    def __init__(self, sock=None, port=8081, hostname='0.0.0.0', max_conns=100, num_threads=4, num_high_priority_threads=1, debug_queues=False):
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((hostname, port))
            sock.listen(max_conns)
            self._sock = sock
        self._sock = sock
        self._queues = {
            'high': queue.Queue(),
            'low': queue.Queue(),
        }
        self._cv = threading.Condition()
        self.request_queue = queue.Queue()
        self._threads = [threading.Thread(target=self._worker_target) for _ in range(num_threads)]
        self._high_priority_threads = [threading.Thread(target=self._high_priority_target) for _ in range(num_high_priority_threads)]

        self._queue_monitor = threading.Thread(target=self._monitor_queues) if debug_queues else None
        self._running = True


    def run(self):
        logger.info('Starting %s worker threads', len(self._threads))
        for t in self._threads:
            t.start()

        logger.info('Starring %s high priority worker threads', len(self._high_priority_threads))
        for t in self._high_priority_threads:
            t.start()

        if self._queue_monitor:
            self._queue_monitor.start();

        while self._running:
            logger.debug("Waiting for connection...")
            (clientsocket, address) = self._sock.accept()
            logger.info("Connection from %s", address)
            # Maybe add to a queue instead?
            threading.Thread(target=self._request_triage, args=(clientsocket,)).start()

    def clean(self):
        self._running = False
        self._sock.close()

    def _worker_target(self):
        while self._running:
            self._cv.acquire()
            while not self._something_in_queue():
                self._cv.wait()
                logger.debug("_worker_target notified")
            request = self._get_existing_request()
            logger.info("worker thread dequeued request(%s)", request)
            self._cv.release()

            self._handle_request(request)

    def _high_priority_target(self):
        while self._running:
            self._cv.acquire()
            while not self._something_in_high_priority_queue():
                self._cv.notify() # we're swallowing a notify if we don't process a low priority request
                self._cv.wait()
                logger.debug("_high_priority_target notified")
            request = self._get_existing_request() # should be guaranteed to be high priority
            logger.info("High priority thread dequeued request(%s)", request)
            self._cv.release()

            self._handle_request(request)

    def _handle_request(self, request):
        body = b'Hello world'
        if request.path == '/ping':
            body = b'healthy'
        elif request.path == '/long':
            logger.debug("Sleeping for one second to simulate a long request")
            time.sleep(1)
            logger.debug('Woke up from thread.sleep')
            body = b'Long long'

        self._write_response(request, body)
        request.sock.close()

    def _monitor_queues(self):
        while self._running:
            for priority in self._queues:
                logger.debug("queues[%s].empty() = %s", priority, self._queues[priority].empty())
            time.sleep(3)

    def _get_existing_request(self):
        # TODO Make more extensible. Should be able to have multiple high queues and other priorities
        if not self._queues['high'].empty():
            return self._queues['high'].get_nowait() # Should be guaranteed
        return self._queues['low'].get_nowait()

    def _something_in_high_priority_queue(self):
        return not self._queues['high'].empty()

    def _something_in_queue(self):
        """Must be called holding the lock"""
        for key in self._queues:
            if not self._queues[key].empty():
                return True
        return False

    def _request_triage(self, clientsocket):
        """Read the first line of the request and store it in the appropriate queue"""
        request = self._get_request(clientsocket)
        logger.info("request(%s)", request)
        self._enqueue_request(request)

    def _enqueue_request(self, request):
        self._cv.acquire()
        queue = self._get_request_queue(request)
        queue.put(request)
        self._cv.notify()
        self._cv.release()

    def _get_request_queue(self, request):
        priority = self._get_request_priority(request)
        logger.debug("%s has priority(%s)", request, priority)
        return self._queues[priority]

    def _get_request_priority(self, request):
        if request.path == '/ping':
            return 'high'
        return 'low'

    def _get_request(self, clientsocket):
        logger.debug("Reading data")
        data = b''
        while True:
            temp_data = clientsocket.recv(1024)
            if not temp_data:
                break

            data += temp_data
            if b'\r\n' in data:
                break

        first_line = str(data.split(b'\r\n')[0], encoding='UTF-8')

        try:
            method, path, version = first_line.split()
            return Request(method=method, path=path, http_version=version, sock=clientsocket)
        except:
            logger.error("First line seems to be wrong '%s'", first_line)
            raise


    def _write_response(self, request, body):
        clientsocket = request.sock
        length = len(body) + 1 # not sure why + 1
        response = b"\n".join([
            b'HTTP/1.1 200 OK',
            b'Content-Length: ' + bytes(str(length), encoding='UTF-8'),
            b"\n",
            body,
        ])
        clientsocket.send(response)


def main():
    server = HttpServerWithPriorities()
    try:
        server.run()
    except:
        server.clean()
        raise


if __name__ == '__main__':
    main()

