import multiprocessing as mp
from queue import Empty
import time
import dagster


def subscriber(jobs: mp.Queue, results: mp.Queue):
    while True:
        try:
            job = jobs.get(timeout=3)            
        except Empty:
            print('no more work')
            break
        
        print(f'received {job}', flush=True)

        # process job
        time.sleep(1)
        
        print(f'done with {job}', flush=True)
        results.put(job + 100)


def publisher():
    
    # set up communication objects
    jobs = mp.Queue(maxsize=5)
    results = mp.Queue(maxsize=10)
    
    # start worker pool
    processes = [mp.Process(target=subscriber, args=(jobs, results )) for i in range(3)]
    for p in processes:
        p.start()

    # send jobs
    for i in range(20):
        print(f'sending {i}')
        jobs.put(i)

    while True:
        try:
            r = results.get(timeout=0.3)
            print(f'received result: {r}')
        except Empty:
            break
    
    print('no more jobs')
    for p in processes:
        p.join()
    

if __name__ == '__main__':
    publisher()