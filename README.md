# Python HTTP Server with Priorities

### Installation
This whole project was build using `Python 3.6.5`. The requirements here
are only for load testing. The server should work out of the box. To test
this out run (from your virtualenv):

```
pip -r requirements.txt
```

### Server Usage
#### Help
```
usage: server.py [-h] [-H HOST] [-p PORT] [-n NUM_THREADS]
                 [-i NUM_HIGH_PRIORITY_THREADS] [-d]

optional arguments:
  -h, --help            show this help message and exit
  -H HOST, --host HOST  host to bind the socket to.
  -p PORT, --port PORT  port to bind the socket to.
  -n NUM_THREADS, --num-threads NUM_THREADS
                        number of worker threads that will be processing http
                        requests
  -i NUM_HIGH_PRIORITY_THREADS, --num-high-priority-threads NUM_HIGH_PRIORITY_THREADS
                        number of high priority threads that will be
                        processing high priority http requests
  -d, --debug-queues    activate an extra thread to report on status of queues
```

#### Examples
##### Default usage
```
python server.py
```

### Load Testing
Load tests rely on [locust](https://github.com/locustio/locust). Make sure
to follow the Installation steps before trying to run this.

Run the following command and point your browser to http://localhost:8089 . Then,
set the number of users, and how many users to add per second (until the
max number of users is reached) and click `Start swarming`.

```
locust --host http://localhost:8081 -f locust/load_test.py LoadTests
```
