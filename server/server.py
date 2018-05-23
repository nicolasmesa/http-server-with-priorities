import socket
import queue
import threading
import logging
import collections
import time

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Container for an HTTP Request
Request = collections.namedtuple('Request', [
    'method',
    'path',
    'http_version',
    'sock',
])


class HttpServerWithPriorities:
    """
    HTTP Server that has different priorities based on the request.
    This is a toy http server that has two queues, one with high priority and one with low priority. All requests go to
    the low priority queue except for the requests that go to '/ping'. The high priority queue will be checked first by
    any thread that is looking for a new request to process thus giving it priority. If `num_high_priority_threads` is
    specified, a thread pool will be created to process only high priority requests.
    """

    def __init__(self, sock=None, port=8081, host='0.0.0.0', num_threads=4, num_high_priority_threads=0, debug_queues=False):
        """
        Constructor for HttpServerWithPriorities. Will create the socket and bind it (if a socket is not provided). Will
        also create the threadpool specified. None of the threads will be started, call the `run()` method to start
        the server.
        :param sock: optionally specify the socket (if not specified, a new socket will be created).
        :param port: port to bind the socket to (only relevant if a socket is not passed).
        :param host: host to bind the socket to (only relevant if a socket is not passed).
        :param num_threads: number of threads that will fulfill the HTTP Requests (should be greater than 0).
        :param num_high_priority_threads: number of threads that will fulfill high priority HTTP Requests (can be zero).
        :param debug_queues: True to create a monitor thread that reports on the status of the queues.
        """
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
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
        """
        Starts the worker threads, the high priority threads and the queue monitor thread (if needed). Then starts
        accepting connections.
        :return: None
        """
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
            (client_socket, address) = self._sock.accept()
            logger.info("Connection from %s", address)
            # Maybe add to a queue instead? Right now we're creating a thread per request to enqueue it.
            threading.Thread(target=self._request_triage, args=(client_socket,)).start()

    def clean(self):
        """
        Stops all threads and closes the sockets.
        :return: None
        """
        logger.info("Stopping the server")
        self._running = False

        self._cv.acquire()
        self._cv.notify_all()
        self._cv.release()
        
        logger.info("Waiting for threads to finish...")
        for t in self._threads:
            t.join()
            
        for t in self._high_priority_threads:
            t.join()
            
        if self._queue_monitor:
            self._queue_monitor.join()
            
        logger.info("Closing socket")
        self._sock.close()

    def _worker_target(self):
        """
        This is the target for the threadpool.
        Checks the queue when notified and processes handles the request. Tries to get from the higher priority queue
        first.
        :return: None
        """
        while self._running:
            self._cv.acquire()
            while not self._something_in_queue():
                if not self._running:
                    self._cv.release()
                    return 
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
                if not self._running:
                    self._cv.release()
                    return 
                self._cv.notify() # we're swallowing a notify if we don't process a low priority request
                self._cv.wait()
                logger.debug("_high_priority_target notified")
            request = self._get_existing_request() # should be guaranteed to be high priority
            logger.info("High priority thread dequeued request(%s)", request)
            self._cv.release()

            self._handle_request(request)

    def _handle_request(self, request):
        """
        Depending on the request path will write a response body. If the request path is '/long', it will sleep to
        emulate a request that takes a while and then will send the response. Also, closes the socket.
        :param request: Request object.
        :return: None
        """
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
        """
        Target for the queue_monitor thread. Checks the state of ach queue and prints a debug message.
        :return: None
        """
        while self._running:
            for priority in self._queues:
                logger.debug("queues[%s].empty() = %s", priority, self._queues[priority].empty())
            time.sleep(3)

    def _get_existing_request(self):
        """
        Gets an existing request from one of the queues. It will try to get the Request from the 'high' priority queue
        first giving it priority. This should be called with a lock and being sure that one of  the queues has one
        request.
        :return: Request the enqueued request.
        """
        if not self._queues['high'].empty():
            return self._queues['high'].get_nowait() # Should be guaranteed
        return self._queues['low'].get_nowait()

    def _something_in_high_priority_queue(self):
        """
        Returns True if there is at least one Request in the 'high' priority queue. Must be called with the lock held.
        :return: boolean
        """
        return not self._queues['high'].empty()

    def _something_in_queue(self):
        """
        Returns True if there is at least one Request in any of the queues. Must be called holding the lock held.
        :return boolean
        """
        for key in self._queues:
            if not self._queues[key].empty():
                return True
        return False

    def _request_triage(self, client_socket):
        """
        Get the request and enqueue it in the right queue.
        :return None
        """
        # might return None if there's an error
        request = self._get_request(client_socket)
        if request:
            logger.info("request(%s)", request)
            self._enqueue_request(request)

    def _enqueue_request(self, request):
        """
        Enqueues the request in the appropriate queue.
        :param request: request to enqueue.
        :return: None
        """
        self._cv.acquire()
        request_queue = self._get_request_queue(request)
        request_queue.put(request)
        self._cv.notify()
        self._cv.release()

    def _get_request_queue(self, request):
        """
        Returns the request queue based on the priority that the request has.
        :param request: request to enqueue.
        :return: queue for the request.
        """
        priority = self._get_request_priority(request)
        logger.debug("%s has priority(%s)", request, priority)
        return self._queues[priority]

    def _get_request_priority(self, request):
        """
        Returns the request priority depending on the request's path.
        :param request: Request to get the priority from.
        :return: str 'high'/'low' depending of the priority of the request.
        """
        if request.path == '/ping':
            return 'high'
        return 'low'

    def _get_request(self, client_socket):
        """
        Reads the first line of the HTTP request and returns a Request object. Returns None if there is an error reading
        or parsing the request.
        :param client_socket: socket to read the HTTP request from.
        :return: Request or None if there is an error.
        """
        logger.debug("Reading data")
        data = b''
        while True:
            temp_data = client_socket.recv(1024)
            if not temp_data:
                break

            data += temp_data
            if b'\r\n' in data:
                break

        first_line = str(data.split(b'\r\n')[0], encoding='UTF-8')

        try:
            method, path, version = first_line.split()
            return Request(method=method, path=path, http_version=version, sock=client_socket)
        except:
            logger.error("First line seems to be wrong '%s'", first_line)
            logger.info("Ignoring error and closing socket")
            client_socket.close()
            return None

    def _write_response(self, request, body):
        """
        Writes the response to a request.
        :param request: request to write a response to.
        :param body: body of the response in bytes.
        :return: None
        """
        client_socket = request.sock
        length = len(body) + 1 # not sure why + 1
        response = b"\n".join([
            b'HTTP/1.1 200 OK',
            b'Connection: Close',
            b'Content-Length: ' + bytes(str(length), encoding='UTF-8'),
            b"\n",
            body,
        ])
        client_socket.send(response)


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument('-H', '--host', required=False, default='0.0.0.0', help='host to bind the socket to.')
    ap.add_argument('-p', '--port', required=False, type=int, default=8081, help='port to bind the socket to.')
    ap.add_argument('-n', '--num-threads', required=False, type=int, default=4, help='number of worker threads that will be processing http requests')
    ap.add_argument('-i', '--num-high-priority-threads', required=False, type=int, default=0, help='number of high priority threads that will be processing high priority http requests')
    ap.add_argument('-d', '--debug-queues', required=False, action='store_true', help='activate an extra thread to report on status of queues')
    
    args = vars(ap.parse_args())
    
    logger.debug("Running with args %s", args)

    server = HttpServerWithPriorities(**args)
    try:
        server.run()
    except KeyboardInterrupt:
        server.clean()


if __name__ == '__main__':
    main()

