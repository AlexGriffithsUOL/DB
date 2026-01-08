import bisect
import struct
from src.indices.pointer_block import PointerBlock
from src.records.structured_records import DataType


PAGE_SIZE = 4096
MAX_KEYS = 100  # adjust as needed

class BTreeNode:
    def __init__(self, page_id, is_leaf, page_allocator, datatype):
        self.page_id = page_id
        self.is_leaf = is_leaf
        self.keys = []      # now list of strings
        self.pointers = []  # list of RIDs lists for duplicates
        self.next_leaf = None
        self.page_allocator = page_allocator
        self.datatype = datatype

    def save(self):
        data = bytearray(PAGE_SIZE)
        data[0] = 1 if self.is_leaf else 0
        struct.pack_into("<H", data, 1, len(self.keys))
        offset = 3

        for key, ptr in zip(self.keys, self.pointers):
            # encoded = key.encode('utf-8')
            # struct.pack_into("<I", data, offset, len(encoded))
            # offset += 4
            # data[offset:offset + len(encoded)] = encoded # data[7:14]
            # offset += len(encoded) # offset += 7
            
            if self.datatype == DataType.STRING:
                encoded = key.encode("utf-8")
                struct.pack_into("<I", data, offset, len(encoded))
                offset += 4
                data[offset:offset + len(encoded)] = encoded
                offset += len(encoded)

            elif self.datatype == DataType.INTEGER:
                struct.pack_into("<I", data, offset, 4)   # length
                offset += 4
                struct.pack_into("<I", data, offset, key) # value
                offset += 4

            else:
                raise TypeError("key must be str or int")

            if self.is_leaf:
                if isinstance(ptr, list):
                    # Inline RID list
                    data[offset] = 0  # type flag
                    offset += 1
                    struct.pack_into("<H", data, offset, len(ptr))
                    offset += 2
                    for pid, slot in ptr:
                        struct.pack_into("<HH", data, offset, pid, slot)
                        offset += 4
                else:
                    # PointerBlock reference
                    data[offset] = 1  # type flag
                    offset += 1
                    struct.pack_into("<I", data, offset, ptr)
                    offset += 4
            else:
                # Internal node: child pointers stored separately
                # nothing here; keys only
                # it appears we are not saving any pointers which is fucking up the loading
                # Inline RID list
                data[offset] = 0  # type flag
                offset += 1
                struct.pack_into("<H", data, offset, len(self.pointers))
                offset += 2
                for child_page in self.pointers:
                    struct.pack_into("<H", data, offset, child_page)
                    offset += 4
                pass

        if self.is_leaf:
            struct.pack_into("<I", data, offset, self.next_leaf or 0)

        self.page_allocator.page_manager.write_page(self.page_id, data)


    @classmethod
    def load(cls, page_allocator, page_id, datatype):
        data = page_allocator.page_manager.get_page(page_id).data
        is_leaf = bool(data[0])
        num_keys = struct.unpack_from("<H", data, 1)[0]
        offset = 3

        keys = []
        pointers = []

        if is_leaf:
            for _ in range(num_keys):
                # read key
                if datatype == DataType.INTEGER:
                    offset += 4
                    key = struct.unpack_from("<I", data, offset)[0]
                    offset += 4
                elif datatype == datatype.STRING:
                    length = struct.unpack_from("<I", data, offset)[0]
                    offset += 4
                    key = data[offset:offset+length].decode('utf-8')
                    offset += length
                keys.append(key)

                # read type flag
                flag = data[offset]
                offset += 1

                if flag == 0:
                    # inline RID list
                    rid_len = struct.unpack_from("<H", data, offset)[0]
                    offset += 2
                    rid_list = []
                    for _ in range(rid_len):
                        pid, slot = struct.unpack_from("<HH", data, offset)
                        rid_list.append((pid, slot))
                        offset += 4
                    pointers.append(rid_list)
                else:
                    # PointerBlock reference
                    ptr_page = struct.unpack_from("<I", data, offset)[0]
                    pointers.append(ptr_page)
                    offset += 4
        else:

            # read keys
            offset = 3
            keys = []
            for _ in range(num_keys):
                # length = struct.unpack_from("<I", data, offset)[0]
                # offset += 4
                # key = data[offset:offset+length].decode('utf-8')
                # offset += length
                if datatype == DataType.INTEGER:
                    offset += 4
                    key = struct.unpack_from("<I", data, offset)[0]
                    offset += 4
                elif datatype == datatype.STRING:
                    length = struct.unpack_from("<I", data, offset)[0]
                    offset += 4
                    key = data[offset:offset+length].decode('utf-8')
                    offset += length
                keys.append(key)
                
                offset += 1
                offset += 2
                
                        # internal node: child pointers
            for _ in range(num_keys + 1):
                ptr = struct.unpack_from("<H", data, offset)[0]
                pointers.append(ptr)
                offset += 4

        next_leaf = None
        if is_leaf:
            next_leaf = struct.unpack_from("<I", data, offset)[0] or None

        node = cls(page_id, is_leaf, page_allocator, datatype=datatype)
        node.keys = keys
        node.pointers = pointers
        node.next_leaf = next_leaf
        return node



    # -------------------------------
    # Core Operations
    # -------------------------------
    def insert(self, key, rid):
        if self.is_leaf:
            return self._insert_leaf(key, rid)
        else:
            return self._insert_internal(key, rid)
    
    def _size_of_entry(self, key, rid_list):
        if self.datatype == DataType.STRING:
            key_bytes = key.encode('utf-8')
        elif self.datatype == DataType.INTEGER:
            key_bytes = struct.pack("<I", key)  # store integer as 4 bytes
        else:
            raise TypeError("key must be str or int")
        
        return 4 + len(key_bytes) + 2 + 4 * len(rid_list)
    
    def _append_rid_to_block(self, block_id, rid):
        block = PointerBlock.load(self.page_allocator, block_id)
        
        current_block = block
        current_block_id = block_id
        
        while current_block.overflow_page is not None:
            current_block_id = current_block.overflow_page
            current_block = PointerBlock.load(self.page_allocator, current_block.overflow_page)
        
        if len(current_block.rids) < PointerBlock.MAX_RIDS:
            current_block.rids.append(rid)
            current_block.save(current_block_id)
            
        else:
            # overflow → create new block
            new_block_id = self.page_allocator.allocate_page()
            
            new_block = PointerBlock(
                overflow_page=None, 
                rids=[rid],
                page_allocator=self.page_allocator
            )
            
            new_block.save(new_block_id)
            current_block.overflow_page = new_block_id
            current_block.save(current_block_id) 
    
    def _insert_leaf(self, key, rid):
        pos = bisect.bisect_left(self.keys, key)

        if pos < len(self.keys) and self.keys[pos] == key:
            ptr = self.pointers[pos]
            if isinstance(ptr, int):  # already a PointerBlock page_id
                self._append_rid_to_block(ptr, rid)
            else:  # small list of RIDs in-memory
                ptr.append(rid)
                # check if it overflows the leaf
                if self._size_of_entry(key, ptr) + 3 + 4 > PAGE_SIZE // 2:
                    # move RIDs to PointerBlock
                    block_page_id = self.page_allocator.allocate_page()
                    pb = PointerBlock(
                        overflow_page=None, 
                        rids=list(ptr),
                        page_allocator=self.page_allocator
                    )
                    pb.save(block_page_id)
                    self.pointers[pos] = block_page_id
        else:
            # new key
            self.keys.insert(pos, key)
            self.pointers.insert(pos, [rid])

        # check if node overflows
        size = 3 + 4
        for k, ptr in zip(self.keys, self.pointers):
            if isinstance(ptr, int): # Why the fuck does this happen?
                size += 4 + len(k.encode('utf-8')) + 4  # key + block ref
            else:
                size += self._size_of_entry(k, ptr)
        
        if size > (PAGE_SIZE - 800):
            return self._split_leaf()
        else:
            self.save()
            return None, None


    
    def _split_leaf(self):
        size = 3 + 4  # header + next_leaf
        split_idx = 0

        for i, (k, rids) in enumerate(zip(self.keys, self.pointers)):
            if self.datatype == DataType.STRING:
                key_size = 4 + len(k.encode('utf-8')) + 2 + 4 * len(rids)
            elif self.datatype == DataType.INTEGER:
                key_size = 4 + 4 + 2 + 4 * len(rids) 
            if size + key_size > PAGE_SIZE // 2:
                # always keep at least one key on left
                if i == 0:
                    split_idx = 1
                break
            size += key_size
            split_idx = i + 1

        # sanity check
        if split_idx == 0:
            split_idx = 1

        right_keys = self.keys[split_idx:]
        right_ptrs = self.pointers[split_idx:]
        left_keys = self.keys[:split_idx]
        left_ptrs = self.pointers[:split_idx]

        right_page_id = self.page_allocator.allocate_page()
        right_node = BTreeNode(right_page_id, is_leaf=True, page_allocator=self.page_allocator, datatype=self.datatype)
        right_node.keys = right_keys
        right_node.pointers = right_ptrs
        right_node.next_leaf = self.next_leaf

        self.keys = left_keys
        self.pointers = left_ptrs
        self.next_leaf = right_page_id
        
        if self.pointers == [7, 1735751535]:
            print('ee')

        self.save()
        right_node.save()

        promoted_key = right_keys[0]
        return promoted_key, right_page_id

    def _insert_internal(self, key, rid):
        pos = bisect.bisect_left(self.keys, key)
        child_page = self.pointers[pos]

        child = BTreeNode.load(self.page_allocator, child_page, datatype=self.datatype)
        split_key, new_child_page = child.insert(key, rid)

        if split_key:
            self.keys.insert(pos, split_key)
            self.pointers.insert(pos + 1, new_child_page)
            if len(self.keys) > MAX_KEYS:
                return self._split_internal()

        self.save()
        return None, None

    def _split_internal(self):
        mid = len(self.keys) // 2
        promoted_key = self.keys[mid]

        right_keys = self.keys[mid + 1:]
        right_ptrs = self.pointers[mid + 1:]
        left_keys = self.keys[:mid]
        left_ptrs = self.pointers[:mid + 1]

        right_page_id = self.page_allocator.allocate_page()
        right_node = BTreeNode(right_page_id, is_leaf=False, page_allocator=self.page_allocator, datatype=self.datatype)
        right_node.keys = right_keys
        right_node.pointers = right_ptrs

        self.keys = left_keys
        self.pointers = left_ptrs

        self.save()
        right_node.save()

        return promoted_key, right_page_id

    # def search(self, key):
    #     if self.is_leaf:
    #         for k, rid in zip(self.keys, self.pointers):
    #             if k == key:
    #                 return rid
    #         return None
    #     else:
    #         pos = bisect.bisect_left(self.keys, key)
    #         if pos == len(self.keys) or key < self.keys[pos]:
    #             child_page = self.pointers[pos]
    #         else:
    #             child_page = self.pointers[pos + 1]
    #         child = BTreeNode.load(self.page_allocator, child_page)
    #         return child.search(key)
    
    def search(self, key):
        if self.is_leaf:
            for k, block_id in zip(self.keys, self.pointers):
                if k == key:
                    # collect all RIDs from pointer blocks
                    rids = []
                    current = block_id
                    while current:
                        block = PointerBlock.load(self.page_allocator, current)
                        rids.extend(block.rids) ### Overflow page isn't working ITS NOT FUCKING WORKING THERE IS NOT 1.8k pages available
                        current = block.overflow_page
                    return rids
            return None
        else:
            pos = bisect.bisect_left(self.keys, key)
            child_page = self.pointers[pos] if pos < len(self.keys) and key < self.keys[pos] else self.pointers[pos + 1]
            child = BTreeNode.load(self.page_allocator, child_page, datatype=self.datatype)
            return child.search(key)



        
class BTreeIndex:
    def __init__(self, page_allocator, root_page_id: int = None, datatype = None):
        self.page_allocator = page_allocator
        
        if datatype is not None:
            self.datatype = datatype
        
        if root_page_id is None:
            self.root_page_id = self._create_root_()
        else:
            self.root_page_id = root_page_id
    
    def _load_root_(self):
        root = BTreeNode(self.root_page_id, is_leaf=True, page_allocator=self.page_allocator, datatype=self.datatype)
        root.load(self.page_allocator, self.root_page_id)

    def _create_root_(self):
        page_id = self.page_allocator.allocate_page()
        root = BTreeNode(page_id, is_leaf=True, page_allocator=self.page_allocator, datatype=self.datatype)
        root.save()
        return page_id

    def insert(self, key, rid):
        root = BTreeNode.load(self.page_allocator, self.root_page_id, datatype=self.datatype)
        split_key, new_page = root.insert(key, rid)

        if split_key:
            new_root_id = self.page_allocator.allocate_page()
            new_root = BTreeNode(new_root_id, is_leaf=False, page_allocator=self.page_allocator, datatype=self.datatype)
            new_root.keys = [split_key]
            new_root.pointers = [root.page_id, new_page]
            new_root.save()
            self.root_page_id = new_root_id

    def search(self, key):
        root = BTreeNode.load(self.page_allocator, self.root_page_id, datatype=self.datatype)
        return root.search(key)

    def full_scan(self):
        """Iterate over all keys in order."""
        node = BTreeNode.load(self.page_allocator, self.root_page_id, datatype=self.datatype)
        while not node.is_leaf:
            node = BTreeNode.load(self.page_allocator, node.pointers[0], datatype=self.datatype)

        while node:
            for k, rid in zip(node.keys, node.pointers):
                yield k, rid
            if node.next_leaf:
                node = BTreeNode.load(self.page_allocator, node.next_leaf, datatype=self.datatype)
            else:
                break
