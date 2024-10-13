from typing import Tuple, List


class Interval:
    def __init__(self, interval: List[int]):
        self.intervals: List[List[int]] = [interval]

    def delete(self, interval: List[int]):
        interval_to_remove = None
        for i in self.intervals:
            if interval[0] >= i[0] and interval[1] <= i[1]:
                interval_to_remove = i
                part_one_start, part_one_end = i[0], interval[0] - 1
                part_two_start, part_two_end = interval[1] + 1, i[1]
                if part_one_start <= part_one_end:
                    self.intervals.append([part_one_start, part_one_end])
                if part_two_start <= part_two_end:
                    self.intervals.append([part_two_start, part_two_end])
                break
        if interval_to_remove:
            self.intervals.remove(interval_to_remove)

        self.intervals.sort()

    def query(self, interval_size: int):
        needed_interval = []
        for i in self.intervals:
            size = i[1] - i[0] + 1
            if size >= interval_size:
                needed_interval = [i[0], i[0] + size - 1]
                break
        if needed_interval:
            return needed_interval
        return None

    def insert(self, interval: List[int]):

        self.intervals.append(interval)
        self.intervals.sort()

        merged = []
        start, end = self.intervals[0]

        for current_start, current_end in self.intervals[1:]:

            if current_start <= end + 1:
                end = max(end, current_end)
            else:
                merged.append([start, end])
                start, end = current_start, current_end

        merged.append([start, end])
        self.intervals = merged

    def get_last_interval(self):
        return self.intervals[-1]
