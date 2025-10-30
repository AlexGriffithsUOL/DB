import bisect
import struct

PAGE_SIZE = 4096
MAX_KEYS = 100  # adjust as needed

# class BTreeNode:
#     def __init__(self, page_id, is_leaf, page_allocator):
#         self.page_id = page_id
#         self.is_leaf = is_leaf
#         self.keys = []
#         self.pointers = []  # children (internal) or RIDs (leaf)
#         self.next_leaf = None  # for leaf nodes only
#         self.page_allocator = page_allocator

#     # -------------------------------
#     # Serialization / Deserialization
#     # -------------------------------
#     def save(self):
#         data = bytearray(PAGE_SIZE)
#         data[0] = 1 if self.is_leaf else 0
#         struct.pack_into("<H", data, 1, len(self.keys))
#         offset = 3

#         # store keys
#         for key in self.keys:
#             struct.pack_into("<I", data, offset, key)
#             offset += 4

#         # store pointers
#         if self.is_leaf:
#             # each pointer is a (page_id, slot)
#             for pid, slot in self.pointers:
#                 struct.pack_into("<HH", data, offset, pid, slot)
#                 offset += 4
#         else:
#             # internal nodes: store child page IDs
#             for ptr in self.pointers:
#                 struct.pack_into("<I", data, offset, ptr)
#                 offset += 4

#         # store leaf linkage
#         if self.is_leaf:
#             struct.pack_into("<I", data, offset, self.next_leaf or 0)

#         self.page_allocator.page_manager.write_page(self.page_id, data)

#     @classmethod
#     def load(cls, page_allocator, page_id):
#         data = page_allocator.page_manager.get_page(page_id).data
#         is_leaf = bool(data[0])
#         num_keys = struct.unpack_from("<H", data, 1)[0]
#         offset = 3

#         # read keys
#         keys = [struct.unpack_from("<I", data, offset + i * 4)[0] for i in range(num_keys)]
#         offset += 4 * num_keys

#         # read pointers
#         pointers = []
#         if is_leaf:
#             for _ in range(num_keys):
#                 pid, slot = struct.unpack_from("<HH", data, offset)
#                 pointers.append((pid, slot))
#                 offset += 4
#         else:
#             for _ in range(num_keys + 1):
#                 ptr = struct.unpack_from("<I", data, offset)[0]
#                 pointers.append(ptr)
#                 offset += 4

#         # next leaf pointer
#         next_leaf = None
#         if is_leaf:
#             next_leaf = struct.unpack_from("<I", data, offset)[0] or None

#         node = cls(page_id, is_leaf, page_allocator)
#         node.keys = keys
#         node.pointers = pointers
#         node.next_leaf = next_leaf
#         return node

#     # -------------------------------
#     # Core Operations
#     # -------------------------------
#     def insert(self, key, rid):
#         if self.is_leaf:
#             return self._insert_leaf(key, rid)
#         else:
#             return self._insert_internal(key, rid)

#     def _insert_leaf(self, key, rid):
#         pos = bisect.bisect_left(self.keys, key)
#         if pos < len(self.keys) and self.keys[pos] == key:
#             raise ValueError("Duplicate key")

#         self.keys.insert(pos, key)
#         self.pointers.insert(pos, rid)

#         if len(self.keys) > MAX_KEYS:
#             return self._split_leaf()
#         else:
#             self.save()
#             return None, None

#     def _split_leaf(self):
#         mid = len(self.keys) // 2
#         right_keys = self.keys[mid:]
#         right_ptrs = self.pointers[mid:]
#         left_keys = self.keys[:mid]
#         left_ptrs = self.pointers[:mid]

#         right_page_id = self.page_allocator.allocate_page()
#         right_node = BTreeNode(right_page_id, is_leaf=True, page_allocator=self.page_allocator)
#         right_node.keys = right_keys
#         right_node.pointers = right_ptrs
#         right_node.next_leaf = self.next_leaf

#         self.keys = left_keys
#         self.pointers = left_ptrs
#         self.next_leaf = right_page_id

#         self.save()
#         right_node.save()

#         promoted_key = right_keys[0]
#         return promoted_key, right_page_id

#     def _insert_internal(self, key, rid):
#         pos = bisect.bisect_left(self.keys, key)
#         child_page = self.pointers[pos]

#         child = BTreeNode.load(self.page_allocator, child_page)
#         split_key, new_child_page = child.insert(key, rid)

#         if split_key:
#             self.keys.insert(pos, split_key)
#             self.pointers.insert(pos + 1, new_child_page)
#             if len(self.keys) > MAX_KEYS:
#                 return self._split_internal()

#         self.save()
#         return None, None

#     def _split_internal(self):
#         mid = len(self.keys) // 2
#         promoted_key = self.keys[mid]

#         right_keys = self.keys[mid + 1:]
#         right_ptrs = self.pointers[mid + 1:]
#         left_keys = self.keys[:mid]
#         left_ptrs = self.pointers[:mid + 1]

#         right_page_id = self.page_allocator.allocate_page()
#         right_node = BTreeNode(right_page_id, is_leaf=False, page_allocator=self.page_allocator)
#         right_node.keys = right_keys
#         right_node.pointers = right_ptrs

#         self.keys = left_keys
#         self.pointers = left_ptrs

#         self.save()
#         right_node.save()

#         return promoted_key, right_page_id

#     def search(self, key):
#         if self.is_leaf:
#             for k, rid in zip(self.keys, self.pointers):
#                 if k == key:
#                     return rid
#             return None
#         else:
#             pos = bisect.bisect_left(self.keys, key)
#             if pos == len(self.keys) or key < self.keys[pos]:
#                 child_page = self.pointers[pos]
#             else:
#                 child_page = self.pointers[pos + 1]
#             child = BTreeNode.load(self.page_allocator, child_page)
#             return child.search(key)


# class BTreeIndex:
#     def __init__(self, page_allocator, root_page_id: int = None):
#         self.page_allocator = page_allocator
        
#         if root_page_id is None:
#             self.root_page_id = self._create_root_()
#         else:
#             self.root_page_id = root_page_id
    
#     def _load_root_(self):
#         root = BTreeNode(self.root_page_id, is_leaf=True, page_allocator=self.page_allocator)
#         root.load(self.page_allocator, self.root_page_id)

#     def _create_root_(self):
#         page_id = self.page_allocator.allocate_page()
#         root = BTreeNode(page_id, is_leaf=True, page_allocator=self.page_allocator)
#         root.save()
#         return page_id

#     def insert(self, key, rid):
#         root = BTreeNode.load(self.page_allocator, self.root_page_id)
#         split_key, new_page = root.insert(key, rid)

#         if split_key:
#             new_root_id = self.page_allocator.allocate_page()
#             new_root = BTreeNode(new_root_id, is_leaf=False, page_allocator=self.page_allocator)
#             new_root.keys = [split_key]
#             new_root.pointers = [root.page_id, new_page]
#             new_root.save()
#             self.root_page_id = new_root_id

#     def search(self, key):
#         root = BTreeNode.load(self.page_allocator, self.root_page_id)
#         return root.search(key)

#     def full_scan(self):
#         """Iterate over all keys in order."""
#         node = BTreeNode.load(self.page_allocator, self.root_page_id)
#         while not node.is_leaf:
#             node = BTreeNode.load(self.page_allocator, node.pointers[0])

#         while node:
#             for k, rid in zip(node.keys, node.pointers):
#                 yield k, rid
#             if node.next_leaf:
#                 node = BTreeNode.load(self.page_allocator, node.next_leaf)
#             else:
#                 break


class BTreeNode:
    def __init__(self, page_id, is_leaf, page_allocator):
        self.page_id = page_id
        self.is_leaf = is_leaf
        self.keys = []      # now list of strings
        self.pointers = []  # list of RIDs lists for duplicates
        self.next_leaf = None
        self.page_allocator = page_allocator

    # -------------------------------
    # Serialization / Deserialization
    # -------------------------------
    # def save(self):
    #     data = bytearray(PAGE_SIZE)
    #     data[0] = 1 if self.is_leaf else 0
    #     struct.pack_into("<H", data, 1, len(self.keys))
    #     offset = 3

    #     # store keys as length-prefixed UTF-8
    #     for key in self.keys:
    #         encoded = key.encode('utf-8')
    #         struct.pack_into("<I", data, offset, len(encoded))
    #         offset += 4
    #         data[offset:offset + len(encoded)] = encoded
    #         offset += len(encoded)

    #     # store pointers (leaf only)
    #     if self.is_leaf:
    #         for rid_list in self.pointers:
    #             struct.pack_into("<H", data, offset, len(rid_list))
    #             offset += 2
    #             for pid, slot in rid_list:
    #                 struct.pack_into("<HH", data, offset, pid, slot)
    #                 offset += 4
    #     else:
    #         for ptr in self.pointers:
    #             struct.pack_into("<I", data, offset, ptr)
    #             offset += 4

    #     # next leaf
    #     if self.is_leaf:
    #         struct.pack_into("<I", data, offset, self.next_leaf or 0)

    #     self.page_allocator.page_manager.write_page(self.page_id, data)
    
    def save(self):
        data = bytearray(PAGE_SIZE)
        data[0] = 1 if self.is_leaf else 0
        struct.pack_into("<H", data, 1, len(self.keys))
        offset = 3

        for key, rid_list in zip(self.keys, self.pointers):
            encoded = key.encode('utf-8')
            needed = 4 + len(encoded) + (2 + 4*len(rid_list) if self.is_leaf else 0)
            if offset + needed + 4 > PAGE_SIZE:  # +4 for next_leaf
                raise ValueError("Node too big to save; split first")
            struct.pack_into("<I", data, offset, len(encoded))
            offset += 4
            data[offset:offset + len(encoded)] = encoded
            offset += len(encoded)

            if self.is_leaf:
                struct.pack_into("<H", data, offset, len(rid_list))
                offset += 2
                for pid, slot in rid_list:
                    struct.pack_into("<HH", data, offset, pid, slot)
                    offset += 4

        if self.is_leaf:
            struct.pack_into("<I", data, offset, self.next_leaf or 0)

        self.page_allocator.page_manager.write_page(self.page_id, data)

        
    @classmethod
    def load(cls, page_allocator, page_id):
        data = page_allocator.page_manager.get_page(page_id).data
        is_leaf = bool(data[0])
        num_keys = struct.unpack_from("<H", data, 1)[0]
        offset = 3

        keys = []
        for _ in range(num_keys):
            length = struct.unpack_from("<I", data, offset)[0]
            offset += 4
            key = data[offset:offset+length].decode('utf-8')
            keys.append(key)
            offset += length

        pointers = []
        if is_leaf:
            for _ in range(num_keys):
                rid_count = struct.unpack_from("<H", data, offset)[0]
                offset += 2
                rid_list = []
                for _ in range(rid_count):
                    pid, slot = struct.unpack_from("<HH", data, offset)
                    rid_list.append((pid, slot))
                    offset += 4
                pointers.append(rid_list)
        else:
            for _ in range(num_keys + 1):
                ptr = struct.unpack_from("<I", data, offset)[0]
                pointers.append(ptr)
                offset += 4

        next_leaf = None
        if is_leaf:
            next_leaf = struct.unpack_from("<I", data, offset)[0] or None

        node = cls(page_id, is_leaf, page_allocator)
        node.keys = keys
        node.pointers = pointers
        node.next_leaf = next_leaf
        return node

    # -------------------------------
    # Core Operations
    # -------------------------------
    def insert(self, key, rid):
        
        if len(self.pointers) > 0 and len(self.pointers[0]) >= 1018:
            print('checkpoint')
        if self.is_leaf:
            return self._insert_leaf(key, rid)
        else:
            return self._insert_internal(key, rid)

    # -------------------------------
    # Leaf insertion (supports duplicates)
    # -------------------------------
    # def _insert_leaf(self, key, rid):
    #     # find insertion point
    #     pos = bisect.bisect_left(self.keys, key)

    #     # handle duplicates: append RID
    #     if pos < len(self.keys) and self.keys[pos] == key:
    #         self.pointers[pos].append(rid)
    #     else:
    #         self.keys.insert(pos, key)
    #         self.pointers.insert(pos, [rid])

    #     if len(self.keys) > MAX_KEYS:
    #         return self._split_leaf()
    #     else:
    #         self.save()
    #         return None, None
    
    def _size_of_entry(self, key, rid_list):
        return 4 + len(key.encode('utf-8')) + 2 + 4*len(rid_list)

    def _insert_leaf(self, key, rid):
        pos = bisect.bisect_left(self.keys, key)

        if pos < len(self.keys) and self.keys[pos] == key:
            self.pointers[pos].append(rid)
        else:
            self.keys.insert(pos, key)
            self.pointers.insert(pos, [rid])

        # Calculate total serialized size
        size = 3 + 4  # header + next_leaf
        for k, rids in zip(self.keys, self.pointers):
            size += self._size_of_entry(k, rids)

        if size > PAGE_SIZE:
            # Node too big → split before saving
            return self._split_leaf()
        else:
            self.save()
            return None, None

    # def _split_leaf(self):
    #     mid = len(self.keys) // 2
    #     right_keys = self.keys[mid:]
    #     right_ptrs = self.pointers[mid:]
    #     left_keys = self.keys[:mid]
    #     left_ptrs = self.pointers[:mid]

    #     right_page_id = self.page_allocator.allocate_page()
    #     right_node = BTreeNode(right_page_id, is_leaf=True, page_allocator=self.page_allocator)
    #     right_node.keys = right_keys
    #     right_node.pointers = right_ptrs
    #     right_node.next_leaf = self.next_leaf

    #     self.keys = left_keys
    #     self.pointers = left_ptrs
    #     self.next_leaf = right_page_id

    #     self.save()
    #     right_node.save()

    #     promoted_key = right_keys[0]
    #     return promoted_key, right_page_id
    
    # def _split_leaf(self):
    #     size = 3 + 4  # header + next_leaf
    #     split_idx = 0

    #     for i, (k, rids) in enumerate(zip(self.keys, self.pointers)):
    #         key_size = 4 + len(k.encode('utf-8')) + 2 + 4 * len(rids)
    #         if size + key_size > PAGE_SIZE // 2:  # split roughly in half
    #             break
    #         size += key_size
    #         split_idx = i + 1

    #     right_keys = self.keys[split_idx:]
    #     right_ptrs = self.pointers[split_idx:]
    #     left_keys = self.keys[:split_idx]
    #     left_ptrs = self.pointers[:split_idx]

    #     right_page_id = self.page_allocator.allocate_page()
    #     right_node = BTreeNode(right_page_id, is_leaf=True, page_allocator=self.page_allocator)
    #     right_node.keys = right_keys
    #     right_node.pointers = right_ptrs
    #     right_node.next_leaf = self.next_leaf

    #     self.keys = left_keys
    #     self.pointers = left_ptrs
    #     self.next_leaf = right_page_id

    #     self.save()
    #     right_node.save()

    #     promoted_key = right_keys[0]
    #     return promoted_key, right_page_id
    
    def _split_leaf(self):
        size = 3 + 4  # header + next_leaf
        split_idx = 0

        for i, (k, rids) in enumerate(zip(self.keys, self.pointers)):
            key_size = 4 + len(k.encode('utf-8')) + 2 + 4 * len(rids)
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
        right_node = BTreeNode(right_page_id, is_leaf=True, page_allocator=self.page_allocator)
        right_node.keys = right_keys
        right_node.pointers = right_ptrs
        right_node.next_leaf = self.next_leaf

        self.keys = left_keys
        self.pointers = left_ptrs
        self.next_leaf = right_page_id

        self.save()
        right_node.save()

        promoted_key = right_keys[0]
        return promoted_key, right_page_id



    def _insert_internal(self, key, rid):
        pos = bisect.bisect_left(self.keys, key)
        child_page = self.pointers[pos]

        child = BTreeNode.load(self.page_allocator, child_page)
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
        right_node = BTreeNode(right_page_id, is_leaf=False, page_allocator=self.page_allocator)
        right_node.keys = right_keys
        right_node.pointers = right_ptrs

        self.keys = left_keys
        self.pointers = left_ptrs

        self.save()
        right_node.save()

        return promoted_key, right_page_id

    def search(self, key):
        if self.is_leaf:
            for k, rid in zip(self.keys, self.pointers):
                if k == key:
                    return rid
            return None
        else:
            pos = bisect.bisect_left(self.keys, key)
            if pos == len(self.keys) or key < self.keys[pos]:
                child_page = self.pointers[pos]
            else:
                child_page = self.pointers[pos + 1]
            child = BTreeNode.load(self.page_allocator, child_page)
            return child.search(key)









        
class BTreeIndex:
    def __init__(self, page_allocator, root_page_id: int = None):
        self.page_allocator = page_allocator
        
        if root_page_id is None:
            self.root_page_id = self._create_root_()
        else:
            self.root_page_id = root_page_id
    
    def _load_root_(self):
        root = BTreeNode(self.root_page_id, is_leaf=True, page_allocator=self.page_allocator)
        root.load(self.page_allocator, self.root_page_id)

    def _create_root_(self):
        page_id = self.page_allocator.allocate_page()
        root = BTreeNode(page_id, is_leaf=True, page_allocator=self.page_allocator)
        root.save()
        return page_id

    def insert(self, key, rid):
        root = BTreeNode.load(self.page_allocator, self.root_page_id)
        split_key, new_page = root.insert(key, rid)

        if split_key:
            new_root_id = self.page_allocator.allocate_page()
            new_root = BTreeNode(new_root_id, is_leaf=False, page_allocator=self.page_allocator)
            new_root.keys = [split_key]
            new_root.pointers = [root.page_id, new_page]
            new_root.save()
            self.root_page_id = new_root_id

    def search(self, key):
        root = BTreeNode.load(self.page_allocator, self.root_page_id)
        return root.search(key)

    def full_scan(self):
        """Iterate over all keys in order."""
        node = BTreeNode.load(self.page_allocator, self.root_page_id)
        while not node.is_leaf:
            node = BTreeNode.load(self.page_allocator, node.pointers[0])

        while node:
            for k, rid in zip(node.keys, node.pointers):
                yield k, rid
            if node.next_leaf:
                node = BTreeNode.load(self.page_allocator, node.next_leaf)
            else:
                break
