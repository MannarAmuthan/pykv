import threading


class StoreException(Exception):
    pass


class StoreGetException(StoreException):
    pass


class Store:
    def __init__(self, background_job,
                 background_jobs_frequency_in_seconds):
        self.stop_event = threading.Event()
        self.background_job_thread = None
        self.background_job = background_job
        self.background_jobs_frequency_in_seconds = background_jobs_frequency_in_seconds

    def start_background_jobs(self):
        self.background_job_thread = threading.Thread(target=self.background_job)
        self.background_job_thread.daemon = True
        self.background_job_thread.start()

    def stop_background_jobs(self):
        self.stop_event.set()
        self.background_job_thread.join()
