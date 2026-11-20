"""Bounded search for a productive mix of largest- and smallest-first slots."""
from array import array
from bisect import insort
from collections import deque


class Pending:
    """Keep only sorted SQLite row IDs and the ranks of yielded transfers."""
    def __init__(self, ids):
        self.ids = array("q", ids)
        self.left = 0
        self.right = len(self.ids) - 1
        self.returned = []

    def __len__(self):
        return max(0, self.right - self.left + 1) + len(self.returned)

    def take(self, largest):
        if not self:
            raise IndexError("empty queue")
        if largest:
            if self.returned and (self.left > self.right or self.returned[-1] > self.right):
                rank = self.returned.pop()
            else:
                rank = self.right
                self.right -= 1
        elif self.returned and (self.left > self.right or self.returned[0] < self.left):
            rank = self.returned.pop(0)
        else:
            rank = self.left
            self.left += 1
        return rank, self.ids[rank]

    def put_back(self, rank):
        insort(self.returned, rank)


class AdaptiveMix:
    def __init__(self, workers, initial, window=300):
        self.workers = workers
        self.target = initial
        self.window = window
        self.step = max(1, workers // 16)
        self.direction = 1
        self.reference = None
        self.started = None
        self.signals = None
        self.cooldown = False

    def update(self, now, retained, small_files, signals, active_large, saturated):
        if self.workers <= 2:
            return
        if signals != self.signals:
            # A changed quota, retry or 429 invalidates an A/B comparison.
            self.signals = signals
            self.started = None
            if self.reference is not None:
                self.target = self.reference[0]
                self.reference = None
                self.cooldown = True
                return "discarded noisy probe"
        if not saturated or active_large != self.target:
            self.started = None
            return
        if self.started is None:
            self.started = now, retained, small_files
            return
        when, previous, completed = self.started
        elapsed = now - when
        if elapsed < self.window:
            return
        self.started = None
        rate = (retained - previous) / elapsed
        small_rate = (small_files - completed) / elapsed
        if rate <= 0:
            return
        if self.cooldown:
            self.cooldown = False
            return "cooldown complete"
        if self.reference:
            old_target, old_rate, old_small = self.reference
            close = abs(rate - old_rate) <= old_rate * 0.05
            improves = rate > old_rate * 1.05 or close and (
                small_rate > old_small or small_rate == old_small and self.target < old_target)
            if not improves:
                self.target = old_target
                self.direction *= -1
                self.reference = None
                self.cooldown = True
                return "restored previous mix"
        candidate = self.next_target()
        self.reference = self.target, rate, small_rate
        self.target = candidate
        return "probing mix after %.2f MB/s and %.1f small files/s" % (rate/1e6, small_rate)

    def next_target(self):
        def bounded():
            return min(self.workers-1, max(1, self.target + self.direction * self.step))
        candidate = bounded()
        if candidate == self.target:
            self.direction *= -1
            candidate = bounded()
        return candidate


class RecentSpeed:
    def __init__(self, now, received, retained):
        self.samples = deque([(now, received, retained)])

    def read(self, now, received, retained, remaining):
        self.samples.append((now, received, retained))
        while len(self.samples) > 2 and self.samples[1][0] <= now - 60:
            self.samples.popleft()
        when, old_received, old_retained = self.samples[0]
        elapsed = max(.001, now-when)
        raw = (received - old_received) / elapsed
        useful = max(0, retained - old_retained) / elapsed
        eta = remaining / useful if useful else None
        return raw, useful, 0 if remaining == 0 else eta
