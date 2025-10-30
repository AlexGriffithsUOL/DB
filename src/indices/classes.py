from typing import List, Optional
from src.pages.allocator import PageAllocator

class BTreeNode:
    page_id: int
    is_leaf: bool
    keys: List[int]
    pointers: List[int]
    next_leaf: Optional[int]

    def insert(key, rid): pass
    def _split_leaf(): pass
    def search(key): pass
    def serialize(): pass

class BTreeIndex:
    root_page_id: int
    page_allocator: PageAllocator
    
    def insert(key, rid): pass
    def search(key): pass
    def full_scan(): pass