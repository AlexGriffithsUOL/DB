from src.files import FileManager
from src.config import get_default_config
from .exceptions import NoPageSizeExcConfigException
from ..cache.lru import LRUCache

class Page:
    id: int
    data: bytearray
    dirty: bool
    
    def __init__(self, id: int, data: bytearray, dirty: bool = False):
        self.id = id
        self.data = bytearray(data)
        self.dirty = dirty
    

class PageManager:
    file_manager: FileManager
    cache: LRUCache
    cache_max: int = 100
    highest_page_id = 0
    free_pages = []
    
    def __init__(
            self, 
            file_manager: FileManager = FileManager(get_default_config()),
            cache_capacity: int = 100
        ):
        self.file_manager = file_manager
        self.cache = LRUCache(cache_capacity)
        self.highest_page_id = 0
        self.free_pages = []
        
    def get_page(self, page_id: int = None):
        if page_id in self.cache:
            return self.cache[page_id]

        page_data = self.file_manager.read_page(page_id)
        page = Page(page_id, page_data)
        self.cache[page_id] = page
        return page
    
    def write_page(self, page_id: int, data: bytes):
        page = self.get_page(page_id)
        page.data = data
        page.dirty = True
        self.cache[page_id] = page
        
        if page_id > self.highest_page_id:
            self.highest_page_id = page_id
        
    def flush(self):
        for page_id in self.cache:
            page_obj = self.cache[page_id]
            if page_obj.dirty:
                self.file_manager.write_page(page_id, page_obj.data)
                self.file_manager.flush()
    
    def alloc_next_raw_page(self):
        data = self.file_manager._get_empty_page_data()
        self.write_page(self.highest_page_id + 1, data)
        
    def get_latest_page(self):
        return self.get_page(self.highest_page_id)
            
    def alloc_page(self, page_id):
        data = self.file_manager._get_empty_page_data()
        self.write_page(page_id, data)
        
    def shutdown(self):
        self.flush()
        self.cache.flush()
    