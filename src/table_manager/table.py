from src.records.exceptions import DataRecordNotEnoughFreeSpaceException
from src.records.structured_records import StructuredDataRecordPage, DataType
from src.config import ENDIAN_TYPE
from src.operators.classes import TableCursor, IndexScan
from src.pages.allocator import PageAllocator
from src.transactions.manager import Transaction
from src.transactions.actions import TXAction, TXActionType
from .utils import filter_by_tx_snapshot

class Table:
    def __init__(self, name, schema, first_page_id, page_allocator):
        self.name = name
        self.schema = schema
        self.first_page_id = first_page_id
        self.page_allocator:PageAllocator = page_allocator
        self.indexes = dict()
        
    def __repr__(self):
        return f'Table({self.name}, {self.first_page_id}, {self.schema})'
    
    def insert(self, record: dict, tx: Transaction, deferred_index: bool = True):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        
        current_page_id = structured_page.next_page_id
        
        while current_page_id != -1:
            page = self.page_allocator.get_page(current_page_id)
            structured_page = StructuredDataRecordPage(page.data)
            current_page_id = structured_page.next_page_id

        try:
            if current_page_id == -1: current_page_id = page.id
            slot_id = structured_page.insert_record(self.schema, record, tx)
            
        except DataRecordNotEnoughFreeSpaceException:
            try:
                slot_id = structured_page.insert_record(self.schema, record, tx)
                
            except DataRecordNotEnoughFreeSpaceException:
                new_page = self.page_allocator.get_page()
                structured_page.next_page_id = new_page.id
                current_page_id = new_page.id

                new_structured_page = StructuredDataRecordPage(new_page.data)
                slot_id = new_structured_page.insert_record(self.schema, record, tx)
                self.page_allocator.page_manager.write_page(new_page.id, new_structured_page.data)
                
        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        
        rid = (current_page_id, slot_id)
        
        ta = TXAction(TXActionType.INSERT, self, rid)
        tx.add_to_timeline(ta)
        
        for column_name in record:
            if column_name in self.indexes:
                index = self.indexes[column_name]
                key = record[column_name]
                
                if not deferred_index:
                    index.insert(key, rid)
                
                if deferred_index:
                    tai = TXAction(
                        TXActionType.INDEX_INSERT,
                        self,
                        rid,
                        None,
                        None,
                        index,
                        key
                    )
                    tx.add_to_timeline(tai)
                
        self.page_allocator.page_manager.flush()

        return current_page_id, slot_id
    
    def _read_all_slots(self, structured_page: StructuredDataRecordPage, include_rids = False):
        records = []
        rids = []
        for slot_num in range(structured_page.num_slots):
            if structured_page._slot_deleted(slot_num) == False:
                raw = structured_page.read_slot(slot_num)
                record = self.deserialise(raw)
                records.append(record)
            
            if include_rids:
                rids.append((structured_page.page_id, slot_num))
        
        if include_rids:
            return records, rids        
        
        return records, None
    
    def cursor(self, tx: Transaction) -> TableCursor:
        return TableCursor(self,tx)
    
    def index_scan(self, column_name, key, tx: Transaction):
        return IndexScan(self, self.indexes[column_name], key, tx)
    
    def scan_all_records(self, include_rids=False, tx_snapshot=None):        
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        structured_page.page_id = self.first_page_id
        records = []
        rids = []
        
        while structured_page.next_page_id != -1:
            new_records, new_rids = self._read_all_slots(structured_page, include_rids)
            records += new_records
            
            if new_rids is not None:
                rids += new_rids
            
            page = self.page_allocator.get_page(structured_page.next_page_id)
            structured_page = StructuredDataRecordPage(page.data)
            structured_page.page_id = page.id
        
        new_records, new_rids = self._read_all_slots(structured_page, include_rids)
        records += new_records
        
        if new_rids is not None:
            rids += new_rids

        if include_rids:
            return records, rids
        
        final_records = filter_by_tx_snapshot(records, tx_snapshot)
        
        return final_records


    def deserialise(self, data):
        """Convert bytes back into a Python dict according to the schema."""
        record = {}
        offset = 0
        tx_created = int.from_bytes(data[offset:offset+8], byteorder=ENDIAN_TYPE)
        offset += 8
        tx_deleted = int.from_bytes(data[offset:offset+8], byteorder=ENDIAN_TYPE)
        offset += 8
        record["i$tx_created"] = tx_created
        record["i$tx_deleted"] = tx_deleted

        for name, ftype, length in self.schema.fields:
            match (ftype):
                case (DataType.INTEGER):
                    if offset + 4 > len(data):
                        raise ValueError(f"Not enough bytes for INTEGER field '{name}'")
                    value = int.from_bytes(data[offset:offset + 4], byteorder=ENDIAN_TYPE, signed=True)
                    record[name] = value
                    offset += 4

                case (DataType.STRING):
                    if offset + 2 > len(data):
                        raise ValueError(f"Not enough bytes for STRING length in field '{name}'")
                    length = int.from_bytes(data[offset:offset + 2], byteorder=ENDIAN_TYPE)
                    offset += 2
                    if offset + length > len(data):
                        raise ValueError(f"Not enough bytes for STRING data in field '{name}'")
                    encoded = data[offset:offset + length]
                    record[name] = encoded.decode('utf-8')
                    offset += length
                    
                case (DataType.BOOLEAN):
                    if offset + 2 > len(data):
                        raise ValueError(f"Not enough bytes for STRING length in field '{name}'")
                    length = int.from_bytes(data[offset:offset + 2], byteorder=ENDIAN_TYPE)
                    offset += 2
                    if offset + length > len(data):
                        raise ValueError(f"Not enough bytes for STRING data in field '{name}'")
                    encoded = data[offset:offset + length]
                    
                    word = encoded.decode('utf-8')
                    
                    if word in ('True', 'False'):
                        if word == 'True':
                            record[name] = True
                            
                        if word == 'False':
                            record[name] = False
                        

                    offset += length

                case _:
                    raise ValueError(f"Unsupported data type: {ftype}")
            
        return record
    
    def delete_by_location(self, page_id, slot_id, tx: Transaction):
        page = self.page_allocator.get_page(page_id)
        structured_page = StructuredDataRecordPage(page.data)
        raw = structured_page.read_slot(slot_id)
        record = self.deserialise(raw)
        record["i$tx_deleted"] = tx.id
        serialized_old = structured_page.serialize(self.schema, record, record["i$tx_created"], record["i$tx_deleted"])
        structured_page.update_slot(slot_id, serialized_old)
        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        
    def delete_at(self, page_id, slot_id):
        page = self.page_allocator.get_page(page_id)
        struct_page = StructuredDataRecordPage(page.data)
        struct_page.delete_slot(slot_id)
        
    def insert_at(self, data, page_id, slot_id):
        page = self.page_allocator.get_page(page_id)
        struct_page = StructuredDataRecordPage(page.data)
        raw_data = struct_page.serialize(self.schema, data, data['i$tx_created'], data['i$tx_deleted'])
        struct_page.update_slot(slot_id, raw_data, undelete=True)

    def delete(self, predicate, tx: Transaction, index_column = None, index_value = None, deferred_index: bool = True):
        
        if index_column is not None and index_value is not None and index_column in self.indexes:
            print('found index')
            index = self.indexes[index_column]
            records, locations = self.index_lookup(index_column, index_value, tx.start_snapshot, True)
            
            for record, location in zip(records,locations):
                print(f'{record}, {location}')
                
                self.delete_by_location(location[0], location[1], tx)
                    
                ta = TXAction(
                    action_type=TXActionType.DELETE,
                    table=self,
                    rid=location,
                    old_data=record
                )
                tx.add_to_timeline(ta)
            
                if deferred_index:
                    tai = TXAction(
                        action_type=TXActionType.INDEX_DELETE,
                        table=self,
                        rid=location,
                        old_data=None,
                        new_data=None,
                        index=index,
                        key=index_value
                    )
                    tx.add_to_timeline(tai)
            
            if not deferred_index:
                self.indexes[index_column].delete(index_value)
                
            self.page_allocator.page_manager.flush()
        
        else:
            page = self.page_allocator.get_page(self.first_page_id)
            structured_page = StructuredDataRecordPage(page.data)
            
            while structured_page.next_page_id != -1:
                for slot_num in range(structured_page.num_slots):
                    if structured_page._slot_deleted(slot_num) == False:
                        raw = structured_page.read_slot(slot_num)
                            
                        record = self.deserialise(raw)
                    
                        if predicate(record):
                            record['i$tx_deleted'] = tx.id
                            serialized = structured_page.serialize(self.schema, record, record["i$tx_created"], record["i$tx_deleted"])
                            structured_page.update_slot(slot_num, serialized)
                            
                            ta = TXAction(
                                action_type=TXActionType.DELETE,
                                table=self,
                                rid=location,
                                old_data=record
                            )
                            tx.add_to_timeline(ta)
                
                self.page_allocator.page_manager.write_page(page.id, structured_page.data)
                
                page = self.page_allocator.get_page(structured_page.next_page_id)
                structured_page = StructuredDataRecordPage(page.data)
            
            for slot_num in range(structured_page.num_slots):
                if structured_page._slot_deleted(slot_num) == False:
                    raw = structured_page.read_slot(slot_num)
                            
                    record = self.deserialise(raw)
                    
                    if predicate(record):
                        # structured_page.delete_slot(slot_num)
                        record['i$tx_deleted'] = tx.id
                        serialized = structured_page.serialize(self.schema, record, record["i$tx_created"], record["i$tx_deleted"])
                        structured_page.update_slot(slot_num, serialized)
                        # structured_page.delete_slot(slot_num)
                        ta = TXAction(
                            action_type=TXActionType.DELETE,
                            table=self,
                            rid=location,
                            old_data=record
                        )
                        tx.add_to_timeline(ta)
                        

            self.page_allocator.page_manager.write_page(page.id, structured_page.data)
            self.page_allocator.page_manager.flush()
            
        
    
    def update_by_location(self, page_id, slot_id, new_values: dict, tx: Transaction):
        page = self.page_allocator.get_page(page_id)
        structured_page = StructuredDataRecordPage(page.data)
        raw = structured_page.read_slot(slot_id)
        record = self.deserialise(raw)
        record["i$tx_deleted"] = tx.id
        serialized_old = structured_page.serialize(self.schema, record, record["i$tx_created"], record["i$tx_deleted"])
        structured_page.update_slot(slot_id, serialized_old)
        
        new_record = record.copy()
        new_record.update(new_values)
        new_record["i$tx_created"] = tx.id
        new_record["i$tx_deleted"] = 0
        overflow_page_id, slot_id = self.insert(new_record, tx)
        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        
    def update(self, predicate, new_values: dict, tx: Transaction, index_column = None, index_value = None, deferred_index: bool = True):
        record: dict = self.scan(predicate, tx.start_snapshot, index_column, index_value)
        self.delete(predicate, tx, index_column, index_value, deferred_index)
        record.update(new_values)
        record["i$tx_created"] = tx.id
        self.insert(record, tx, deferred_index)
        
    def _read_row_by_position(self, page_id, slot_id):
        page = self.page_allocator.get_page(page_id)
        structured_page = StructuredDataRecordPage(page.data)
        records = []

        # if structured_page._slot_deleted(slot_id) == False: # where its failing
        
        raw = structured_page.read_slot(slot_id)
        record = self.deserialise(raw)
        records.append(record)
            
        if len(records) == 1:
            return records[0]
        
        return records
    
    def get_index(self, column_name):
        if column_name in self.indexes:
            return self.indexes[column_name]
        
        return None

    def index_lookup(self, column_name, value, tx_snapshot = None, include_locations = False):
        """
        Returns all records matching `value` using the index if it exists,
        """
        index = self.get_index(column_name)
        
        if index is not None:
            locations = index.search(value) 
            index_records = [self._read_row_by_position(*loc) for loc in locations]
            
            
            if include_locations:
                filtered_records, filtered_locations = filter_by_tx_snapshot(index_records, tx_snapshot, locations=locations)
                return filtered_records, filtered_locations
            
            filtered_records = filter_by_tx_snapshot(index_records, tx_snapshot)
            
            return filtered_records
        
    def scan(self, predicates, tx_snapshot,  index_column=None, index_value=None): # Index column etc is for hinting
        if index_column is not None and index_value is not None and index_column in self.indexes:
            all_records = self.index_lookup(index_column, index_value, tx_snapshot)
        else:
            all_records = self.scan_all_records(include_rids=False, tx_snapshot=tx_snapshot)
            
        records = [x for x in all_records if predicates(x)]
        
        if len(records) == 1:
            return records[0]
        
        return records
        
    def range_scan(self, column_name, lower_bound, upper_bound):
        index = self.get_index(column_name)
        
        if index is not None:
            locations = index.range_scan(lower_bound,upper_bound)
            return [self._read_row_by_position(*loc) for loc in locations]
        else:
            return [r for r in self.scan_all_records() if r[column_name] < upper_bound and r[column_name > lower_bound]]
        
        