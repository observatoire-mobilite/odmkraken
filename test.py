import multiprocessing as mp
from queue import Empty
import time
import dagster
import uuid
import random
import collections
import contextlib


def compute(job):
    t = random.random() * 0.1
    time.sleep(t)
    return t


def subscriber(jobs: mp.Queue, done: mp.Queue):
    while True:
        try:
            job = jobs.get(timeout=1)
        except Empty:
            print('no more work')
            break
        
        # process job
        t = compute(job)
        
        done.put((mp.current_process().pid, t), timeout=10)

    print('exiting')
    return


class ResultGatherer:

    def __init__(self):
        self.results = []

    def count(self) -> collections.Counter:
        time = {}
        for r in self.results:
            time[r[0]] = time.get(r[0], 0) + r[1]
        return time

    def gather(self, results):
        while True:
            try:
                res = results.get_nowait()
                self.results.append(res)
            except Empty:
                break

    def total(self):
        return sum(r[1] for r in self.results)
            


def publisher():
    
    # set up communication objects
    jobs = mp.Queue(maxsize=10)
    done = mp.Queue()
    
    # start worker pool
    processes = [mp.Process(target=subscriber, args=(jobs, done )) for i in range(3)]
    for p in processes:
        p.start()

    results = ResultGatherer()
    pending_jobs = iter(uuid.uuid4() for i in range(1500))
    for job in pending_jobs:
        results.gather(done)
        jobs.put(job)
    
    print('no more jobs')
    results.gather(done)
    for p in processes:
        p.join()
    print(results.count())
    print(results.total())



class Processor:

    def __init__(self):
        self.t = 0

    def process_result(self, t):
        self.t += t


def main():

    pending_jobs = iter(uuid.uuid4() for i in range(1500))
    p = Processor()
    with mp.Pool() as pool:
        for job in pending_jobs:
            res = pool.apply_async(compute, args=(job,), callback=p.process_result)
        res.get()
    print(p.t)
    
if __name__ == '__main__':
    t0 = time.perf_counter()
    main()
    publisher()
    print(time.perf_counter() - t0)
