from locust import HttpLocust, TaskSet, task


class Tasks(TaskSet):
    min_wait = 5000
    max_wait = 15000

    @task(1)
    def slow_request(self):
        self.client.get('/long')

    @task(2)
    def healthcheck(self):
        self.client.get('/ping')

    @task(1)
    def normal_request(self):
        self.client.get('/')


class LoadTests(HttpLocust):
    task_set = Tasks