import bisect
import struct
from src.indices.pointer_block import PointerBlock
from src.records.structured_records import DataType


PAGE_SIZE = 4096
MAX_KEYS = 100

class AlexIndex:
    def __init__(self, datatype, page_allocator, root_page_id):
        self.page_allocator = page_allocator
        self.datatype = datatype
        self.root_page_id = root_page_id