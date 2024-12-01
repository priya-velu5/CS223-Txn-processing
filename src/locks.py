# src/locks.py

import threading

class LockManager:
    def __init__(self):
        self.locks = {}
        self.lock = threading.Lock()

    def acquire(self, resource):
        """
        Acquire a lock on the given resource.
        """
        with self.lock:
            if resource not in self.locks:
                self.locks[resource] = threading.Lock()
            resource_lock = self.locks[resource]
        resource_lock.acquire()
        return True

    def release(self, resource):
        """
        Release the lock on the given resource.
        """
        with self.lock:
            if resource in self.locks:
                self.locks[resource].release()

    def release_all(self, resources):
        """
        Release all locks held on a list of resources.
        """
        for resource in resources:
            self.release(resource)
