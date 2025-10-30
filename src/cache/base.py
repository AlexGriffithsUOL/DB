import random

class Cache:
    def __init__(self, capacity: int = 100):
        self.cache = dict()
        self.capacity = capacity
        
    def __getitem__(self, key):
        if key in self.cache:
            return self.cache[key]
        return None
    
    def __setitem__(self, key, value):
        self.cache[key] = value
        self.evict(recent_key=key)
        
    def __iter__(self):
        self._keys = list(self.cache.keys())
        self._index = 0
        return self
    
    def __next__(self):
        if self._index < len(self._keys):
            key = self._keys[self._index]
            self._index += 1
            return key
        else:
            raise StopIteration
        
    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        return None
    
    def put(self, key, value):
        self.cache[key] = value
        self.evict(recent_key=key)
        
    def evict(self, key=None, recent_key=None):
        if len(self.cache) > self.capacity:
            if key is None:
                key_choices = list(self.cache.keys())
                
                if recent_key is not None:
                    key_choices.remove(recent_key)
                    
                key = random.choice(key_choices)
            del self.cache[key]
            
    def flush(self):
        self.cache = dict()