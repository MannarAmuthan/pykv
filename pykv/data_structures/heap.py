from heapq import heappush, heappop
from queue import PriorityQueue
from typing import Tuple


class Heap:
    def __init__(self):
        self.queue = PriorityQueue()

    def push(self, element: Tuple[int, str]):
        self.queue.put(element)

    def peek(self):
        if not self.is_empty():
            return self.queue.queue[0]
        return None

    def is_empty(self):
        if self.queue.empty():
            return True
        return False

    def pop(self):
        return self.queue.get()
