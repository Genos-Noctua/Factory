#Factory 1.6
import multiprocessing as mp
import threading, time
from tqdm import tqdm

class Package:
    def __init__(self):
        self.dst = 0
        self.con = {}

class Factory:
    def __init__(self, tasks, processes=mp.cpu_count()):
        self.tasks = tasks
        self.stop_flag = False
        self.stream = mp.Queue()
        self.drain = mp.Queue()
        self.pool = mp.Pool(processes=processes)
        self.runner = threading.Thread(target=self.run, args = ())
        self.runner.daemon = True
        self.runner.start()

    def run(self):
        while not self.stop_flag:
            if self.stream.empty():
                time.sleep(0.1)
                continue
            pack = self.stream.get()
            self.pool.apply_async(self.tasks[pack.dst], args=(pack,), callback=self.export)
        
    def map(self, packs, verbal = False, desc = 'Mapping...'):
        y = len(packs)
        if y == 0:
            return packs
        for pack in packs:
            self.add(pack)
        packs.clear()
        for _ in tqdm(range(y), smoothing=0, desc=desc, disable=not verbal):
            packs.append(self.take())
        return packs

    def export(self, pack):
        if pack.dst == 'out':
            self.drain.put(pack)
        elif pack.dst == 'rip':
            del pack
        else: 
            self.stream.put(pack)

    def add(self, pack): 
        if isinstance(pack, list):
            for x in pack:
                self.stream.put(x)
            return
        self.stream.put(pack)

    def take(self): return self.drain.get()

    def get_packs(num):
        if isinstance(num, list): num = len(num)
        return [Package() for _ in range(num)]

    def kill(self):
        self.stop_flag = True
        self.runner.join()
        self.stream.close()
        self.pool.close()
        self.pool.join()
        self.drain.close()


'''
from factory import *
import random

def multiplier(package):
    for i in range(100000):
        package.payload['mul'] = package.payload['x'] * package.payload['y']
    package.dst = 1
    return package

def summer(package):
    for i in range(100000):
        package.payload['sum'] = package.payload['x'] + package.payload['y']
    package.dst = 'rip'
    return package

if __name__ == '__main__':
    factory = Factory((multiplier, summer), processes=4)
    for x in range(100):
        pack = Package()
        pack.payload = {'x': random.randint(1, 10), 'y': random.randint(1, 10), 'z': random.randint(1, 10)}
        factory.add(pack)
    factory.kill()
    del factory, pack, x
'''

'''
from factory import *

def lol(package):
    if not isinstance(package.payload['x'], list):
        package.payload['x'] = [package.payload['x'],]
    for x in package.payload['x']:
        print(x)
        
    package.dst = 'out'
    return package

if __name__ == '__main__':
    factory = Factory((lol, ), processes=8)
    factory.map(list(range(10)), 'x', 'each')
    factory.map(list(range(10)), 'x', 'batch')
    factory.kill()
    del factory
'''
