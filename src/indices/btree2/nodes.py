from typing import List
from src.pages import Page
from src.datatypes.classes import DataTypeBaseType
import math
import bisect

class BTreeNode(Page):
    IS_LEAF = (0, 1)
    PREVIOUS_LEAF = (1,5)
    NEXT_LEAF = (5,9)
    
    NUMBER_OF_KEYS = (9, 13)
    DATA_START_IDX = 13

    PAGE_PTR_SIZE = 8
    SLOT_PTR_SIZE = 8
    PAGE_SLOT_PTR_SIZE = PAGE_PTR_SIZE + SLOT_PTR_SIZE
    
    @property
    def _min_data_end_(self):
        return len(self.data) - 1
    
    @property
    def _max_pointers_(self):
        return self.MAX_KEYS + 1
    
    @property
    def is_leaf(self):
        data = self.deserialise_ptr(*self.IS_LEAF)
        return bool(data)
    
    @is_leaf.setter
    def is_leaf(self, val):
        if val not in (True, False, 0, 1):
            raise Exception('WRONG')
        
        numeric_repr = int(val)
        encoded = int.to_bytes(numeric_repr, 1, 'little', signed=False)
        self.write_data(*self.IS_LEAF, encoded)
        
    @property
    def next_leaf(self):
        return self.deserialise_ptr(*self.NEXT_LEAF)
    
    @next_leaf.setter
    def next_leaf(self, value):
        data = self.serialise_ptr(value, 4)
        self.write_data(*self.NEXT_LEAF, data)
        
    @property
    def previous_leaf(self):
        return self.deserialise_ptr(*self.PREVIOUS_LEAF)
    
    @previous_leaf.setter
    def previous_leaf(self, value):
        data = self.serialise_ptr(value, 4)
        self.write_data(*self.PREVIOUS_LEAF, data)
        
    @property
    def number_of_keys(self):
        return self.deserialise_ptr(*self.NUMBER_OF_KEYS)
    
    @number_of_keys.setter
    def number_of_keys(self, val):
        data = self.serialise_ptr(val, 4)
        self.write_data(*self.NUMBER_OF_KEYS, data)
        
    @property
    def key_size(self):
        return self.datatype.length
        
    @property
    def key_pointer_block_size(self):
        size = self.key_size + self.PAGE_PTR_SIZE
        if self.is_leaf:
            return size + self.SLOT_PTR_SIZE
        else:
            return size
        
    @property
    def MAX_KEYS(self):
        available_data = len(self.data) - self.DATA_START_IDX
        if self.is_leaf:
            max_keys = available_data // self.key_pointer_block_size
            return max_keys
        else:
            max_keys = (available_data - self.PAGE_PTR_SIZE) // self.key_pointer_block_size
            return max_keys
        
    def serialise_ptr(self, ptr, size):
        return int.to_bytes(ptr, size, 'little', signed=False)
    
    def deserialise_ptr(self, start, end):
        data = self.data[start:end]
        return int.from_bytes(data, 'little', signed=False)
    
    def serialise_key(self, key):
        return self.datatype.serialise(key)
    
    def deserialise_key(self, start, end):
        data = self.data[start:end]
        return self.datatype.deserialise(data)
    
    def write_data(self, start, end, data):
        self.data[start:end] = data
        
    def flush(self):
        self.page_allocator.page_manager.write_page(self.id, self.data)
        
    def deserialise(self):
        keys = []
        pointers = []
        
        if self.is_leaf:
            for i in range(self.number_of_keys):
                key_start = self.DATA_START_IDX + (i * self.key_pointer_block_size)
                key_end = key_start + self.key_size
                page_pointer_start = key_end
                page_pointer_end = page_pointer_start + self.PAGE_PTR_SIZE
                slot_pointer_start = page_pointer_end
                slot_pointer_end = slot_pointer_start + self.SLOT_PTR_SIZE
                
                
                key = self.deserialise_key(key_start, key_end)
                page_pointer = self.deserialise_ptr(page_pointer_start, page_pointer_end)
                slot_pointer = self.deserialise_ptr(slot_pointer_start, slot_pointer_end)
                
                keys.append(key)
                pointers.append((page_pointer, slot_pointer))
        
        else:
            for i in range(self.number_of_keys):
                page_pointer_start = self.DATA_START_IDX + (i * self.key_pointer_block_size)
                page_pointer_end = page_pointer_start + self.PAGE_PTR_SIZE
                key_start = page_pointer_end
                key_end = key_start + self.key_size
                
                pointer = self.deserialise_ptr(page_pointer_start, page_pointer_end)
                
                key = self.deserialise_key(key_start, key_end)
                
                pointers.append(pointer)
                keys.append(key)
                
                if (i+1) == self.number_of_keys:
                    page_pointer_start = key_end
                    page_pointer_end = page_pointer_start + self.key_pointer_block_size
                    
                    pointer = self.deserialise_ptr(page_pointer_start, page_pointer_end)

                    pointers.append(pointer)
                    
        self.keys = keys
        self.pointers = pointers
        
    def set_data(self, data):
        self.data = data
    
    def __init__(
        self,
        datatype,
        id,
        page_allocator,
        data = None,
        is_leaf = None
        ):
        self.datatype:DataTypeBaseType = datatype
        self.keys = []
        self.pointers = []
        self.id = id
        
        if data is not None:
            self.data = data
            
        if data is None:
            self.data = page_allocator.get_page(self.id).data
            
        if is_leaf is not None:
            self.is_leaf = is_leaf

        self.page_allocator = page_allocator
        
        self.deserialise()

    def save(self):
        if self.is_leaf:
            num_key = self.number_of_keys
            next_leaf = self.next_leaf
            previous_leaf = self.previous_leaf
            self.data = self.page_allocator.page_manager.file_manager._get_empty_page_data()
            
            self.number_of_keys = num_key
            self.is_leaf = True
            self.next_leaf = next_leaf
            self.previous_leaf = previous_leaf
            
            for i in range(self.number_of_keys):
                key_start = self.DATA_START_IDX + (i * self.key_pointer_block_size)
                key_end = key_start + self.key_size
                page_pointer_start = key_end
                page_pointer_end = page_pointer_start + self.PAGE_PTR_SIZE
                slot_pointer_start = page_pointer_end
                slot_pointer_end = slot_pointer_start + self.SLOT_PTR_SIZE
                
                key = self.serialise_key(self.keys[i])
                
                rid = self.pointers[i]
                page_ptr = self.serialise_ptr(rid[0], self.PAGE_PTR_SIZE)
                slot_ptr = self.serialise_ptr(rid[1], self.SLOT_PTR_SIZE)
                
                self.write_data(key_start, key_end, key)
                self.write_data(page_pointer_start, page_pointer_end, page_ptr)
                self.write_data(slot_pointer_start, slot_pointer_end, slot_ptr)
        
        else:
            num_key = self.number_of_keys
            self.data = self.page_allocator.page_manager.file_manager._get_empty_page_data()
            self.number_of_keys = num_key
            self.is_leaf = False
            for i in range(self.number_of_keys):
                page_pointer_start = self.DATA_START_IDX + (i * self.key_pointer_block_size)
                page_pointer_end = page_pointer_start + self.PAGE_PTR_SIZE
                key_start = page_pointer_end
                key_end = key_start + self.key_size
                
                page_ptr = self.serialise_ptr(self.pointers[i], self.PAGE_PTR_SIZE)
                key = self.serialise_key(self.keys[i])

                self.write_data(page_pointer_start, page_pointer_end, page_ptr)
                self.write_data(key_start, key_end, key)
                
                if (i+1) == self.number_of_keys:
                    final_idx = i+1
                    page_pointer_start = key_end
                    page_pointer_end = page_pointer_start + self.PAGE_PTR_SIZE
                    
                    page_ptr = self.serialise_ptr(self.pointers[final_idx], self.PAGE_PTR_SIZE)
                    
                    self.write_data(page_pointer_start, page_pointer_end, page_ptr)
        self.flush()
        
    def _split_internal(self):
        mid = len(self.keys) // 2
        promoted_key = self.keys[mid]

        right_keys = self.keys[mid + 1:]
        right_ptrs = self.pointers[mid + 1:]
        left_keys = self.keys[:mid]
        left_ptrs = self.pointers[:mid + 1]

        right_page_id = self.page_allocator.allocate_page()
        right_node = BTreeNode(self.datatype, right_page_id, self.page_allocator, is_leaf=False)
        right_node.keys = right_keys
        right_node.pointers = right_ptrs
        right_node.number_of_keys = len(right_keys)

        self.keys = left_keys
        self.pointers = left_ptrs
        self.number_of_keys = len(left_keys)

        self.save()
        right_node.save()

        return promoted_key, right_page_id
        
    def _split_leaf_(self):
        split_idx = len(self.keys) // 2

        if split_idx == 0:
            split_idx = 1

        right_keys = self.keys[split_idx:]
        right_ptrs = self.pointers[split_idx:]
        left_keys = self.keys[:split_idx]
        left_ptrs = self.pointers[:split_idx]

        right_page = self.page_allocator.get_page()
        right_node = BTreeNode(
            datatype=self.datatype,
            id=right_page.id,
            data=right_page.data,
            page_allocator=self.page_allocator,
            is_leaf=True
        )
        right_node.keys = right_keys
        right_node.pointers = right_ptrs
        right_node.number_of_keys = len(right_keys)

        self.keys = left_keys
        self.pointers = left_ptrs
        self.number_of_keys = len(left_keys) # oh my god we never ever insert the key?
        self.next_leaf = right_page.id
        right_node.previous_leaf = self.id

        self.save()
        right_node.save()

        promoted_key = right_keys[0]
        return promoted_key, right_page.id
    
    def _insert_leaf_(self,key,rid):
        position = bisect.bisect_right(self.keys, key)
        
        self.number_of_keys += 1
        self.keys.insert(position, key)
        self.pointers.insert(position, rid)
        self.save()
        
        if self.number_of_keys >= self.MAX_KEYS:
            return self._split_leaf_()
        
        return None, None
    
    def _insert_internal(self, key, rid):
        pos = bisect.bisect_right(self.keys, key)
        child_page = self.pointers[pos]
        
        child = BTreeNode(self.datatype, child_page, self.page_allocator)
        split_key, new_child_page = child.insert(key, rid)

        if split_key:
            self.keys.insert(pos, split_key)
            self.number_of_keys += 1
            self.pointers.insert(pos + 1, new_child_page)
            if len(self.keys) > self.MAX_KEYS:
                return self._split_internal()

        self.save()
        return None, None
    
    def _increment_no_keys(self, amount = 1):
        self.number_of_keys += amount
        
    def _decrement_no_keys(self, amount = 1):
        self.number_of_keys -= amount
        
    def prepend(self, key, pointer):
        self.keys.insert(0, key)
        self.pointers.insert(0, pointer)
        self._increment_no_keys()
        
    def append(self, key, pointer):
        self.keys.append(key)
        self.pointers.append(pointer)
        self._increment_no_keys()
            
    def insert(self, key, rid):
        if self.is_leaf:
            return self._insert_leaf_(key, rid)
        else:
            return self._insert_internal(key, rid)
        
    def update_nearest_key(self, key):
        new_key_idx = bisect.bisect_right(self.keys, key)
        # new_key_idx = bisect.bisect_left(self.keys, key)
        # self.keys[new_key_idx] = key
        if key == 940:
            print('yay')
        if len(self.keys) > 1:
            self.keys[new_key_idx] = key
        else:
            self.keys[0] = key
        
        
    def get_leaf(self, leaf_number):
        if leaf_number > 3:
            return BTreeNode(
                self.datatype, leaf_number, self.page_allocator, is_leaf=True
            )
        else:
            return None
        
    def get_child_node(self, page_id):
        return BTreeNode(
            self.datatype,
            page_id,
            self.page_allocator
        )
    
    def get_next_leaf(self):
        return self.get_leaf(self.next_leaf)
        
    def get_previous_leaf(self):
        return self.get_leaf(self.previous_leaf)
    
    def get_siblings(self):
        return self.get_previous_leaf(), self.get_next_leaf()
        
    def search(self, key, ret_leaf=False, return_stack: List=None):
        if self.is_leaf:
            if ret_leaf:
                if return_stack:
                    return self, return_stack
                else:
                    return self
            
            indexes = [i for i, v in enumerate(self.keys) if v == key]
            
            results = []
            
            for idx in indexes:
                results.append(self.pointers[idx])
            
            return results
        else:
            pos = bisect.bisect_right(self.keys, key)
            
            if len(self.keys) == 1:
                if key >= self.keys[0]:
                    child_page = self.pointers[1]
                else:
                    child_page = self.pointers[0]
            else:
                child_page = self.pointers[pos]
            
            child = BTreeNode(self.datatype, child_page, self.page_allocator)
            child.parent = self
            
            if return_stack is not None:
                return_stack.append((self, pos))
                
            return child.search(key, ret_leaf, return_stack)
    
    @property
    def minimum_keys(self):
        if self.is_leaf:
            return math.ceil(self.MAX_KEYS / 2)
        
        else:
            return math.ceil((self.MAX_KEYS + 1) / 2) - 1
    
    @property
    def has_enough_keys(self):
        return len(self.keys) >= self.minimum_keys
    
    @property
    def can_lend_key(self):
        return len(self.keys) > self.minimum_keys
    
    def debug_print_info(self):
        print(f'Node [{self.id}], Keys: {self.keys[0]} - {self.keys[-1]}')
    
    def pop(self, idx):
        key = self.keys.pop(idx)
        rid = self.pointers.pop(idx)
        self._decrement_no_keys()
        return key, rid
    
    def remove_leaf_key_pointer(self, key, rid=None):
        idx = self.keys.index(key)
        if rid is None:    
            self.pop(idx)
        else:
            self.keys.pop(idx)
            pointer_idx = self.pointers.index(rid)
            self.pointers.pop(pointer_idx)
            self.number_of_keys -= 1
    
    def right_pop(self):
        return self.pop(-1)
    
    def left_pop(self):
        return self.pop(0)
        
    def search_with_path(self, key):
        stack = []
        leaf_node, stack = self.search(key, ret_leaf=True, return_stack=stack)
        return leaf_node, stack