from pathlib import Path
from .files.exceptions import UnderMinimumPageSizeException

MINIMUM_PAGE_SIZE = 2048
ENDIAN_TYPE = 'little'
DEFAULT_DB_NAME = 'pysql'
NULL_PAGE_ID_VALUE = -1

class DBConfig:
    db_location: str | Path
    page_size_kb: int = 4096
    auto_create: bool = False
    max_file_open_retries: int = 3
    
    def __init__(
        self, 
        db_location: str | Path = None, 
        page_size_kb: int | None = 4096,
        auto_create: bool = False,
        max_file_open_retries: int = 3
        ):
        if isinstance(db_location, str):
            db_location = Path(db_location)
            
        if isinstance(db_location, Path):
            db_location = str(db_location.resolve())
            
        self.auto_create = auto_create
        self.db_location = db_location
        self.max_file_open_retries = max_file_open_retries
        
        if page_size_kb < MINIMUM_PAGE_SIZE:
            raise UnderMinimumPageSizeException(MINIMUM_PAGE_SIZE, page_size_kb)
        
def get_default_config():
    return DBConfig(f'./{DEFAULT_DB_NAME}.db', auto_create=True)