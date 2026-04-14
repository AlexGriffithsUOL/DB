from collections import OrderedDict
from .base import Cache
from src.console.logging_config import setup_logging
import logging

setup_logging()

logger = logging.getLogger('LRU Cache')
logger.debug('Cache imported')

class LRUCache(Cache):
    def __contains__(self, key):
        if key in self.cache:
            return True
        return False
    
    def __getitem__(self, key):
        return self.get(key)
    
    def __setitem__(self, key, value):
        self.put(key, value)
    
    def __init__(self, capacity: int, eviction_hook = None):
        self.cache = OrderedDict()
        self.capacity = capacity
        
        if eviction_hook is not None:
            self.eviction_hook = eviction_hook
        
        logger.debug('Cache instanciated')
        
    def last_key(self):
        return list(self.cache.keys())[0]

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
            logger.debug('Evicting')
          
            first_key = self.last_key()
            
            if self.eviction_hook is not None:
                self.eviction_hook(first_key) # Need to work on thisdebu
            
            key = self.cache.popitem()
            logger.debug(f'{key} evicted')
            