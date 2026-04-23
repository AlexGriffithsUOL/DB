import bisect
from .string_nodes import BTreeNode
from .nodes import BTreeNode as NumericNode
from .utils import Directions
from src.datatypes.classes import StringDataType, IntegerDataType
        
class BTreeIndex:
    def __init__(self, page_allocator, root_page_id: int = None, datatype = None):
        self.page_allocator = page_allocator
        
        if datatype is not None:
            self.datatype = datatype
            
        if isinstance(self.datatype, StringDataType):
            self.node_class = BTreeNode
        else:
            self.node_class = NumericNode
        
        if root_page_id is None:
            self.root_page_id = self._create_root_()
        else:
            self.root_page_id = root_page_id
    
    def get_root(self):
        return self.node_class(
            self.datatype,
            self.root_page_id,
            self.page_allocator
        )

    def _create_root_(self):
        page = self.page_allocator.get_page()
        root = self.node_class(
            datatype=self.datatype,
            id=page.id,
            page_allocator=self.page_allocator,
            is_leaf=True
        )
        root.save()
        return page.id

    def insert(self, key, rid):
        root = self.node_class(
            self.datatype,
            self.root_page_id,
            self.page_allocator
        )
        split_key, new_page = root.insert(key, rid)

        if split_key:
            print(f'Split key found, page id: {new_page}, key: {split_key}')
            new_root = self.page_allocator.get_page()
            new_root = self.node_class(
                datatype=self.datatype,
                page_allocator=self.page_allocator,
                id=new_root.id,
                data=new_root.data
            )

            new_root.keys = [split_key]
            new_root.pointers = [root.id, new_page]
            new_root.number_of_keys = 1
            new_root.save()
            self.root_page_id = new_root.id
            return self.root_page_id
        else:
            return None
    
    def search(self, key):
        root = self.node_class(self.datatype, self.root_page_id, self.page_allocator)
        return root.search(key)
    
    def _find_low_leaf(self, low):
        root = self.node_class(
            self.datatype, self.root_page_id, self.page_allocator
        )
        
        return root.search(low, ret_leaf=True)
    
    def range_scan(self, low, high):
        leaf = self._find_low_leaf(low)
        results = []

        while leaf:
            for key, value in zip(leaf.keys, leaf.pointers):
                if key < low:
                    continue
                if key > high:
                    return results
                results.append(value)
            leaf = leaf.get_next_leaf()
        return results
    
    def _merge_leaf_node(self, source_node: BTreeNode, target_node: BTreeNode, direction: Directions):
        if direction == Directions.RIGHT:
            
            end_node = target_node.get_next_leaf()
            
            source_node.keys += target_node.keys
            source_node.pointers += target_node.pointers
            source_node.number_of_keys = len(source_node.keys)
            target_node.number_of_keys = 0
            
            target_node.is_leaf = False
            target_node.keys.clear()
            target_node.pointers.clear()
            
            source_node.next_leaf = target_node.next_leaf
            target_node.next_leaf = 0
            target_node.previous_leaf = 0
            
            if end_node is not None:
                end_node.previous_leaf = source_node.id
            
            source_node.save()
            target_node.save()
            
            if end_node is not None:
                end_node.save()
            
        if direction == Directions.LEFT:
            
            end_node = target_node.get_previous_leaf()
            
            source_node.keys = target_node.keys + source_node.keys
            source_node.pointers = target_node.pointers + source_node.pointers
            source_node.number_of_keys = len(source_node.keys)
            target_node.number_of_keys = 0
            
            target_node.is_leaf = False
            target_node.keys.clear()
            target_node.pointers.clear()
            source_node.previous_leaf = target_node.previous_leaf
            
            target_node.next_leaf = 0
            target_node.previous_leaf = 0
            
            if end_node is not None:
                end_node.next_leaf = source_node.id
            
            source_node.save()
            target_node.save()
            
            if end_node is not None:
                end_node.save()

    def _handle_underflow(self, node, path):
        """
        Handle underflow in a B+ Tree node (leaf or internal).print(
        'node' is the node that may be underflowing.
        'path' is the list of nodes from root to this node (optional for recursion).
        """
        if node.id == self.root_page_id:
            if len(node.pointers) == 1 and isinstance(node.pointers[0], int):
                print('found root!')
                self.root_page_id = node.pointers[0]
                return
            
        # If node has enough keys or is root, nothing to do
        if node.has_enough_keys or not hasattr(node, 'parent'):
            return

        parent = node.parent
        current_idx = parent.pointers.index(node.id)        

        # Identify siblings
        left_sibling = None
        left_sibling_id = parent.pointers[current_idx - 1] if current_idx > 0 else None
        
        right_sibling = None
        right_sibling_id = parent.pointers[current_idx + 1] if current_idx < len(parent.pointers) - 1 else None
            
        if left_sibling_id is not None:
            left_sibling = parent.get_child_node(left_sibling_id)
            
        if right_sibling_id is not None:
            right_sibling = parent.get_child_node(right_sibling_id)

        # ---------------- Borrowing ----------------
        # Borrow from left sibling
        if left_sibling and left_sibling.can_lend_key:
            key, value = left_sibling.right_pop()
            node.pointers.insert(0, value)
            
            if current_idx == len(parent.keys):
                current_idx -= 1
            elif current_idx > 0:
                current_idx -= 1

            # Update parent separator key
            parent_separator = parent.keys[current_idx]
            node.keys.insert(0, parent_separator)
            node.number_of_keys += 1
            leftest_node = node.get_child_node(node.pointers[0])
            
            parent.keys[current_idx] = leftest_node.keys[0]

            left_sibling.save()
            node.save()
            parent.save()
            return

        # Borrow from right sibling
        if right_sibling and right_sibling.can_lend_key:
            key, value = right_sibling.left_pop()
            node.pointers.append(value)
            node.number_of_keys += 1
            
            parent_separator = parent.keys[current_idx] # WE NEED TO MAKE THIS BECOME A NEW SEPERATOR IF WE MOVE THIS ACROSS
            node.keys.append(parent_separator)

            # Update parent separator key
            parent.keys[current_idx] = key

            right_sibling.save()
            node.save()
            parent.save()
            return

        # Merge with left sibling
        if left_sibling:
            self._merge_leaf_node(node, left_sibling, Directions.LEFT)
    
            merged_parent: BTreeNode = parent
            idx = merged_parent.pointers.index(left_sibling_id)
            pointer_idx = idx
            
            if idx == len(merged_parent.pointers):
                idx -= 1
            
            merged_parent.pointers.pop(pointer_idx)
            key = merged_parent.keys.pop(idx)
            merged_parent._decrement_no_keys()
            merged_parent.save()
            
            temp_idx1 = bisect.bisect_right(node.keys, key)
            
            node.keys.insert(temp_idx1, key)
            node.number_of_keys += 1
            node.save()
                
            if not merged_parent.has_enough_keys:
                self._handle_underflow(merged_parent, path)
            return

        # If no siblings exist (should not happen unless root)
        if parent is None:
            return

        # Merge with right sibling
        if right_sibling:
            self._merge_leaf_node(node, right_sibling, Directions.RIGHT)
            
            merged_parent = parent
            idx = merged_parent.pointers.index(right_sibling_id)
            pointer_idx = idx
            
            if idx == len(merged_parent.pointers):
                idx -= 1
            else:
                idx -= 1
                
            merged_parent.pointers.pop(pointer_idx)
            key = merged_parent.keys.pop(idx)
            merged_parent.number_of_keys -= 1
            merged_parent.save()
            
            temp_idx1 = bisect.bisect_right(node.keys, key)
            
            node.keys.insert(temp_idx1, key)
            node.number_of_keys += 1
            node.save()
                
            if not merged_parent.has_enough_keys:
                self._handle_underflow(merged_parent, path)
            return

        # If no siblings exist (should not happen unless root)
        if parent is None:
            return
        
    def borrow(self, node: BTreeNode, sibling: BTreeNode, parent: BTreeNode, direction: Directions):
        match(direction):
            case(Directions.LEFT):
                key, rid = sibling.right_pop()
                node.prepend(key, rid)
                
            case(Directions.RIGHT):
                key, rid = sibling.left_pop()
                node.append(key, rid)
                
        parent.update_nearest_key(key) # if breaks then please please make the right borrow decrement the index
        # in the key update by one or smthing
                
        sibling.save()
        node.save()
        parent.save()
    
    def left_borrow(self, node, sibling, parent):
        print('left borrow')
        self.borrow(node, sibling, parent, Directions.LEFT)
        
    def right_borrow(self, node, sibling, parent):
        print('right borrow')
        self.borrow(node, sibling, parent, Directions.RIGHT)
        
    def merge(self, node: BTreeNode, sibling: BTreeNode, parent: BTreeNode, path:list, direction: Directions):
        print('yay merging')
        if parent:
            pointer_idx = parent.pointers.index(sibling.id)
    
    def delete(self, key, rid=None):
        root = self.get_root()
        parent: BTreeNode

        leaf, path = root.search_with_path(key)
        
        parent, child_idx = path.pop() if path else (None, None)
        
        leaf.remove_leaf_key_pointer(key, rid)
        leaf.save()
        
        if not leaf.has_enough_keys:
            left_sibling, right_sibling = leaf.get_siblings()
            
            if left_sibling and left_sibling.can_lend_key:
                self.left_borrow(leaf, left_sibling, parent)
                return self._handle_underflow(parent, path)
                
            if right_sibling and right_sibling.can_lend_key:
                self.right_borrow(leaf, right_sibling, parent)
                return self._handle_underflow(parent, path)
            
            if left_sibling:
                self._merge_leaf_node(leaf, left_sibling, Directions.LEFT)

                if parent:
                    pointer_idx = parent.pointers.index(left_sibling.id)
                    key_idx = bisect.bisect_left(parent.keys, key)
                    
                    if key_idx > 0:
                        key_idx -= 1
                        
                    parent.keys.pop(key_idx)
                    parent.pointers.pop(pointer_idx)
                    parent.number_of_keys -= 1
                    
                    if right_sibling:
                        if key_idx == len(parent.keys):
                            key_idx -= 1
                        parent.keys[key_idx] = right_sibling.keys[0]
                
                    parent.save()
                    
                return self._handle_underflow(parent, path)
                
            if right_sibling:
                self._merge_leaf_node(leaf, right_sibling, Directions.RIGHT)
                
                if parent:
                    key_idx = bisect.bisect_right(parent.keys, key)
                    pointer_idx = key_idx + 1
                    
                    parent.keys.pop(key_idx)
                    parent.pointers.pop(pointer_idx)
                    parent.number_of_keys -= 1
                    
                    if left_sibling:
                        parent.keys[key_idx] = left_sibling.keys[-1]
                        
                    parent.save()
                    
                    
                return self._handle_underflow(parent, path)