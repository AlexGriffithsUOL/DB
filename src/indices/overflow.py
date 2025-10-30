import struct
from typing import List, Any
from src.pages.allocator import PageManager

class OverflowPage:
    PAGE_SIZE = 4096

    def __init__(
        self, 
        page_id: int, 
        page_allocator: PageManager, 
        rids=None, 
        next_page=None
    ):
        self.page_id = page_id
        self.page_allocator = page_allocator
        self.rids: List = rids or []
        self.next_page = next_page

    def save(self):
        data = bytearray(self.PAGE_SIZE)
        struct.pack_into("<H", data, 0, len(self.rids))
        offset = 2
        for pid, slot in self.rids:
            struct.pack_into("<HH", data, offset, pid, slot)
            offset += 4
            if offset + 4 > self.PAGE_SIZE:
                raise ValueError("Too many RIDs for a single overflow page")
        struct.pack_into("<I", data, offset, self.next_page or 0)
        self.page_allocator.page_manager.write_page(self.page_id, data)

    @classmethod
    def load(cls, page_allocator, page_id):
        data = page_allocator.page_manager.get_page(page_id).data
        num_rids = struct.unpack_from("<H", data, 0)[0]
        rids = []
        offset = 2
        for _ in range(num_rids):
            pid, slot = struct.unpack_from("<HH", data, offset)
            rids.append((pid, slot))
            offset += 4
        next_page = struct.unpack_from("<I", data, offset)[0] or None
        return cls(page_id, page_allocator, rids, next_page)
