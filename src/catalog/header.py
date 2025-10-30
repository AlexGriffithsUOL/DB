from src.pages.allocator import PageAllocator
from src.config import ENDIAN_TYPE

class CatalogHeader:
    def __init__(self, page_allocator: PageAllocator = None):
        
        if page_allocator is None:
            page_allocator = PageAllocator()
        
        self.page_allocator = page_allocator
        self.page = page_allocator.get_page(0)
        self.init()
    
    def init(self):
        if self.database_version != 'PYDB':
            raise Exception("Invalid database file")
        
        print('PYDB Version: ' + self.database_version)
        
        self.bitmap_start_id = int.from_bytes(self.page.data[6:10], ENDIAN_TYPE)
        self.sys_table_pid = int.from_bytes(self.page.data[10:14], ENDIAN_TYPE)
        self.sys_columns_pid = int.from_bytes(self.page.data[14:18], ENDIAN_TYPE)
        
    @property
    def database_version(self):
        return self.page.data[0:4].decode('utf-8')
    
    @property
    def table_counter(self):
        return int.from_bytes(self.page.data[18:34], ENDIAN_TYPE)
    
    @table_counter.setter
    def table_counter(self, val):
        self.page.data[18:34] = int(val).to_bytes(16, ENDIAN_TYPE)
        self.page_allocator.page_manager.write_page(0, self.page.data)