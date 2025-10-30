from .exceptions import (
    BaseFileManagerException, 
    FileManagerNoConfigException,
    FileManagerReadPageException,
    UnderMinimumPageSizeException,
    CannotOpenFileException
)

from ..config import get_default_config, DBConfig

class FileManager:
    config: DBConfig = None
    file = None
    open_attempts: int = 0
    
    def __init__(self, config: DBConfig = get_default_config()):
        self.config = config
        self.open_attempts = 0
        self.open_file()
        
    def create_file(self):
        with open(self.config.db_location, 'wb') as file:
            file.close()
            
    def open_file(self):
        try:
            self.file = open(self.config.db_location, 'r+b')
        except:
            if self.config.auto_create and self.open_attempts < self.config.max_file_open_retries:
                self.create_file()
                self.open_file()
            else:
                raise CannotOpenFileException()
            
    def read_page(self, page_id: int):
        self.file.seek(page_id * self.config.page_size_kb)
        return self.file.read(self.config.page_size_kb)
    
    def _get_empty_page_data(self):
        return bytearray(b''.ljust(self.config.page_size_kb, b'\x00'))
    
    def write_page(self, page_id: int, data):
        assert len(data) == self.config.page_size_kb
        self.file.seek(page_id * self.config.page_size_kb)
        self.file.write(data)
        self.flush()
        
    def flush(self):
        self.file.flush()
        
    def close(self):
        self.file.close()