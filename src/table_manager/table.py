from src.records.exceptions import DataRecordNotEnoughFreeSpaceException
from src.records.structured_records import StructuredDataRecordPage, DataType
from src.config import ENDIAN_TYPE
from src.pages.allocator import PageAllocator

class Table:
    def __init__(self, name, schema, first_page_id, page_allocator):
        self.name = name
        self.schema = schema
        self.first_page_id = first_page_id
        self.page_allocator:PageAllocator = page_allocator
        self.indexes = dict()
        
    def __repr__(self):
        return f'Table({self.name}, {self.first_page_id}, {self.schema})'
    
    def _filter_by_tx_snapshot(self, records, tx_snapshot=None):
        final_records = []
        for record in records:
            if tx_snapshot is None:
                # no filtering
                final_records.append(record)
            else:
                if record["_tx_created"] <= tx_snapshot and (record["_tx_deleted"] == 0 or record["_tx_deleted"] > tx_snapshot):
                    final_records.append(record)
                    
        return final_records
    
    def insert(self, record: dict, tx_id):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        
        current_page_id = structured_page.next_page_id
        
        while current_page_id != -1:
            page = self.page_allocator.get_page(current_page_id)
            structured_page = StructuredDataRecordPage(page.data)
            current_page_id = structured_page.next_page_id

        try:
            if current_page_id == -1: current_page_id = page.id
            slot_id = structured_page.insert_record(self.schema, record, tx_id)
            
        except DataRecordNotEnoughFreeSpaceException:
            try:
                slot_id = structured_page.insert_record(self.schema, record, tx_id)
                
            except DataRecordNotEnoughFreeSpaceException:
                new_page = self.page_allocator.get_page()
                structured_page.next_page_id = new_page.id
                current_page_id = new_page.id

                new_structured_page = StructuredDataRecordPage(new_page.data)
                slot_id = new_structured_page.insert_record(self.schema, record, tx_id)
                self.page_allocator.page_manager.write_page(new_page.id, new_structured_page.data)
                
        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        
        self.page_allocator.page_manager.flush()
        
        for column_name in record:
            if column_name in self.indexes:
                self.indexes[column_name].insert(record[column_name], (current_page_id, slot_id))

        return current_page_id, slot_id
    
    def _read_all_slots(self, structured_page: StructuredDataRecordPage, include_rids = False):
        records = []
        rids = []
        for slot_num in range(structured_page.num_slots):
            if structured_page._slot_deleted(slot_num) == False:
                raw = structured_page.read_slot(slot_num)
                record = self.deserialize(raw)
                records.append(record)
            
            if include_rids:
                rids.append((structured_page.page_id, slot_num))
        
        if include_rids:
            return records, rids        
        
        return records, None
    
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
        
        final_records = self._filter_by_tx_snapshot(records, tx_snapshot)
        
        return final_records


    def deserialize(self, data):
        """Convert bytes back into a Python dict according to the schema."""
        record = {}
        offset = 0
        tx_created = int.from_bytes(data[offset:offset+8], byteorder=ENDIAN_TYPE)
        offset += 8
        tx_deleted = int.from_bytes(data[offset:offset+8], byteorder=ENDIAN_TYPE)
        offset += 8
        record["_tx_created"] = tx_created
        record["_tx_deleted"] = tx_deleted

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

    def delete(self, predicate, tx_id):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        
        while structured_page.next_page_id != -1:
            for slot_num in range(structured_page.num_slots):
                if structured_page._slot_deleted(slot_num) == False:
                    raw = structured_page.read_slot(slot_num)
                        
                    record = self.deserialize(raw)
                
                    if predicate(record):
                        record['_tx_deleted'] = tx_id
                        serialized = structured_page.serialize(self.schema, record, record["_tx_created"], record["_tx_deleted"])
                        structured_page.update_slot(slot_num, serialized)
                        # structured_page.delete_slot(slot_num)
            
            self.page_allocator.page_manager.write_page(page.id, structured_page.data)
            
            page = self.page_allocator.get_page(structured_page.next_page_id)
            structured_page = StructuredDataRecordPage(page.data)
        
        for slot_num in range(structured_page.num_slots):
            if structured_page._slot_deleted(slot_num) == False:
                raw = structured_page.read_slot(slot_num)
                        
                record = self.deserialize(raw)
                
                if predicate(record):
                    structured_page.delete_slot(slot_num)

        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        self.page_allocator.page_manager.flush()
        
        
    def update(self, predicate, new_values: dict, tx_id):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        
        page_id_array = []
        page_id_array.append(self.first_page_id)
        
        while structured_page.next_page_id != -1:
            page_id_array.append(structured_page.next_page_id)
            for slot_num in range(structured_page.num_slots):
                if structured_page._slot_deleted(slot_num) == False:
                    raw = structured_page.read_slot(slot_num)
                        
                    record = self.deserialize(raw)
                
                    if predicate(record):
                        
                        for value in new_values:
                            if value in self.indexes:
                                rid = (page.id, slot_num)
                                old_value = record[value]
                                new_value = new_values[value]
                                
                                self.indexes[value].delete(old_value, rid) # maintain the fcking index
                                self.indexes[value].insert(new_value, rid)
                        
                        
                        # record.update(new_values)
                        # serialized = structured_page.serialize(self.schema, record)
                        # structured_page.update_slot(slot_num, serialized)
                        
                        record["_tx_deleted"] = tx_id
                        serialized_old = structured_page.serialize(self.schema, record, record["_tx_created"], record["_tx_deleted"])
                        structured_page.update_slot(slot_num, serialized_old)
                        
                        # Insert new version
                        new_record = record.copy()
                        new_record.update(new_values)
                        new_record["_tx_created"] = tx_id
                        new_record["_tx_deleted"] = 0
                        overflow_page_id, slot_id = self.insert(new_record, tx_id)
                        # page_id, slot_id = structured_page.insert_record(self.schema, new_record, tx_id)
            
            self.page_allocator.page_manager.write_page(page.id, structured_page.data)
            
            page = self.page_allocator.get_page(structured_page.next_page_id)
            structured_page = StructuredDataRecordPage(page.data)
        
        for slot_num in range(structured_page.num_slots):
            if structured_page._slot_deleted(slot_num) == False:
                raw = structured_page.read_slot(slot_num)
                
                record = self.deserialize(raw)
                
                if predicate(record):
                    # record.update(new_values)
                    record["_tx_deleted"] = tx_id
                    serialized_old = structured_page.serialize(self.schema, record, record["_tx_created"], record["_tx_deleted"])
                    structured_page.update_slot(slot_num, serialized_old)

                    # Insert new version
                    new_record = record.copy()
                    new_record.update(new_values)
                    new_record["_tx_created"] = tx_id
                    new_record["_tx_deleted"] = 0
                    overflow_page_id, slot_id = self.insert(new_record, tx_id)
                    
                    for key, value in new_values.items():
                        if key in self.indexes:
                            self.indexes[key].update(value)
                        
                    # serialized = structured_page.serialize(self.schema, record, tx_id)
                    # structured_page.update_slot(slot_num, serialized)

        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        self.page_allocator.page_manager.flush()
        
    def _read_row_by_position(self, page_id, slot_id):
        page = self.page_allocator.get_page(page_id)
        structured_page = StructuredDataRecordPage(page.data)
        records = []

        if structured_page._slot_deleted(slot_id) == False:
            raw = structured_page.read_slot(slot_id)
            record = self.deserialize(raw)
            records.append(record)
            
        if len(records) == 1:
            return records[0]
        
        return records
    
    def get_index(self, column_name):
        if column_name in self.indexes:
            return self.indexes[column_name]
        
        return None

    def index_lookup(self, column_name, value, tx_snapshot):
        """
        Returns all records matching `value` using the index if it exists,
        otherwise falls back to a full table scan.
        """
        index = self.get_index(column_name)
        
        if index is not None:
            locations = index.search(value) 
            index_records = [self._read_row_by_position(*loc) for loc in locations]
            return self._filter_by_tx_snapshot(index_records, tx_snapshot)
            # return [self._read_row_by_position(*loc) for loc in locations]
        # else:
            # fallback: full scan
            # return [r for r in self.scan_all_records() if r[column_name] == value]
        
    def old_scan(self, column_name, value):
        if column_name in self.indexes:
            return self.index_lookup(column_name, value)
        
        else:
            all_records = self.scan_all_records()
            records = [x for x in all_records if x[column_name] == value]
            
            if len(records) == 1:
                return records[0]
            
            return records
        
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
        
        