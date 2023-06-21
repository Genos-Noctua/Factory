#Factory 1.5
import multiprocessing as mp
import threading, time

class Package:
    def __init__(self):
        self.dst = 0
        self.payload = {}

class Factory:
    def __init__(self, tasks, pressure=100, processes=mp.cpu_count()):
        self.tasks = tasks
        self.pressure = pressure
        self.processes = processes
        self.stop_flag = False
        self.stream = mp.Queue(maxsize=pressure)
        self.drain = mp.Queue()
        self.pack_pool = mp.Queue(maxsize=100000)
        for x in range(100):
            self.pack_pool.put(Package())
        self.pool = mp.Pool(processes=self.processes)
        self.runner = threading.Thread(target=self.run, args = ())
        self.runner.daemon = True
        self.runner.start()

    def map(self, packs):
        y = len(packs)
        for pack in packs:
            self.add(pack)
        packs = list()
        for _ in range(y):
            packs.append(self.take())
        return packs

    def run(self):
        while not self.stop_flag:
            if self.stream.empty():
                time.sleep(0.01)
                continue
            pack = self.stream.get()
            self.pool.apply_async(self.tasks[pack.dst], args=(pack,), callback=self.export)
        
    def export(self, pack):
        if pack.dst == 'out':
            self.drain.put(pack)
        elif pack.dst == 'rip':
            self.pack_pool.put(pack)
        else: 
            self.stream.put(pack)

    def add(self, pack): self.stream.put(pack)

    def take(self): return self.drain.get()

    def get_pack(self):
        x = self.pack_pool.get()
        x.dst = 0
        x.payload = {}
        return x 

    def get_packs(self, num): 
        if isinstance(num, list): num = len(num)
        return [self.get_pack() for _ in range(num)]

    def ret_pack(self, pack): self.pack_pool.put(pack)

    def kill(self):
        self.stop_flag = True
        self.runner.join()
        self.stream.close()
        self.pool.close()
        self.pool.join()
        self.drain.close()
        self.pack_pool.close()


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
        pack = factory.get_pack()
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
    factory = Factory((lol, ), processes=8, pressure=10)
    factory.map(list(range(10)), 'x', 'each')
    factory.map(list(range(10)), 'x', 'batch')
    factory.kill()
    del factory
'''