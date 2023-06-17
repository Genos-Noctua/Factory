#Factory 1.4
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
        self.pack_pool = mp.Queue(maxsize=100)
        for x in range(100):
            self.pack_pool.put(Package())
        self.pool = mp.Pool(processes=self.processes)
        self.runner = threading.Thread(target=self.run, args = ())
        self.runner.daemon = True
        self.runner.start()

    def map(self, elements, name, mode='each'):
        if mode=='each':
            ready=0
            while len(elements) in range(len(elements), 1, -1):
                y = 0
                for _ in range(self.processes*2):
                    if len(elements) == 0:
                        break
                    pack = self.get_pack()
                    pack.payload = {name: elements.pop()}
                    self.add(pack)
                    y+=1
                for _ in range(y):
                    pack = self.take()
                    self.ret_pack(pack)

        elif mode=='batch':
            ready=0
            pros = self.processes
            lists = [[] for _ in range(pros)]
            for x in range(len(elements)):
                lists[x % pros].append(elements[x])
            for x in range(pros):
                pack = self.get_pack()
                pack.payload = {name: lists[x]}
                self.add(pack)
            while ready < pros:
                pack = self.take()
                self.ret_pack(pack)
                ready+=1
            del lists

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

    def add(self, pack):
        self.stream.put(pack)

    def take(self):
        return self.drain.get()

    def get_pack(self):
        x = self.pack_pool.get()
        x.dst = 0
        x.payload = {}
        return x 

    def ret_pack(self, pack):
        self.pack_pool.put(pack)

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
