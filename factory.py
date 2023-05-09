#Factory 1.0
import multiprocessing as mp
import threading

class Package:
    def __init__(self):
        self.dst = 0
        self.special = {}
        self.payload = {}

class Factory:
    def __init__(self, tasks, pressure=100, processes=8):
        self.tasks = tasks
        self.pressure = pressure
        self.processes = processes
        self.stop_flag = False
        self.stream = mp.Queue(maxsize=pressure)
        self.pack_pool = mp.Queue(maxsize=processes*2)
        for x in range(processes*2):
            self.pack_pool.put(Package())
        self.pool = mp.Pool(processes=self.processes)
        self.runner = threading.Thread(target=self.run, args = ())
        self.runner.daemon = True
        self.runner.start()

    def run(self):
        while not self.stop_flag:
            pack = self.stream.get()
            self.pool.apply_async(self.tasks[pack.dst], args=(pack,), callback=self.export)
        
    def export(self, pack):
        if pack.dst != -1:
            self.stream.put(pack)
        else: self.ret_pack(pack)

    def add(self, pack):
        self.stream.put(pack)

    def get_pack(self):
        x = self.pack_pool.get()
        x.dst = 0
        x.special = {}
        x.payload = {}
        return x 

    def ret_pack(self, pack):
        self.pack_pool.put(pack)

    def kill(self):
        self.stop_flag = True
        self.runner.join()
        self.pool.close()
        self.pool.join()
        self.stream.close()
        self.pack_pool.close()


'''
from factory import *
import random

def multiplier(package):
    for i in range(100000):
        package.payload['mul'] = package.special['x'] * package.special['y']
    package.dst = 1
    return package

def summer(package):
    for i in range(100000):
        package.payload['sum'] = package.special['x'] + package.special['y']
    package.dst = -1
    return package

if __name__ == '__main__':
    factory = Factory((multiplier, summer), processes=4, pressure=10)
    for x in range(100):
        pack = factory.get_pack()
        pack.special = {'x': random.randint(1, 10), 'y': random.randint(1, 10), 'z': random.randint(1, 10)}
        factory.add(pack)
    factory.kill()
    del factory, pack, x
'''
