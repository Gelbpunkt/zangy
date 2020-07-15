# Simple benchmarking utility lib
# to be compatible with benchmark.js
import time


class timeout(object):
    def __init__(self, seconds):
        self.seconds = seconds

    def __enter__(self):
        self.die_after = time.time() + self.seconds
        return self

    def __exit__(self, type, value, traceback):
        pass

    @property
    def timed_out(self):
        return time.time() > self.die_after


class Suite:
    def __init__(self):
        self.funcs = []
        self.listeners = {}

    def add(self, description, function):
        self.funcs.append((description, function))

    def on(self, event, function):
        self.listeners[event] = function

    def run(self, delay=1, min_samples=150):
        for desc, func in self.funcs:
            time.sleep(delay)
            total_ops = 0
            with timeout(10) as t:
                start_time = time.time()
                for _ in range(min_samples):
                    func()
                    total_ops += 1
                while not t.timed_out:
                    func()
                    total_ops += 1
            end_time = time.time()
            ops_per_sec = round(total_ops / (end_time - start_time))

            if listener := self.listeners.get("cycle"):
                listener(
                    "{} x {:,} ops/sec ({} runs sampled)".format(
                        desc, ops_per_sec, total_ops
                    )
                )
