from collections import OrderedDict
from .base import Cache

class LRUCache(Cache):
    def __contains__(self, key):
        if key in self.cache:
            return True
        return False
    
    def __getitem__(self, key):
        return self.get(key)
    
    def __setitem__(self, key, value):
        self.put(key, value)
    
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key not in self.cache:
            return None
        
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
            
        self.cache[key] = value
        self.evict()

    def evict(self):
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)