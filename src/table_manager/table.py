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
        
    def __repr__(self):
        return f'Table({self.name}, {self.first_page_id}, {self.schema})'
    
    def insert(self, record: dict):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        
        current_page_id = structured_page.next_page_id
        
        while current_page_id != -1:
            page = self.page_allocator.get_page(current_page_id)
            structured_page = StructuredDataRecordPage(page.data)
            current_page_id = structured_page.next_page_id

        try:
            if current_page_id == -1: current_page_id = page.id
            slot_id = structured_page.insert_record(self.schema, record)
            
        except DataRecordNotEnoughFreeSpaceException:
            try:
                slot_id = structured_page.insert_record(self.schema, record)
                
            except DataRecordNotEnoughFreeSpaceException:
                new_page = self.page_allocator.get_page()
                structured_page.next_page_id = new_page.id
                current_page_id = new_page.id

                new_structured_page = StructuredDataRecordPage(new_page.data)
                slot_id = new_structured_page.insert_record(self.schema, record)
                self.page_allocator.page_manager.write_page(new_page.id, new_structured_page.data)
                
        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        
        self.page_allocator.page_manager.flush()

        return current_page_id, slot_id
    
    def scan_all_records(self):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        records = []
        
        while structured_page.next_page_id != -1:
            for slot_num in range(structured_page.num_slots):
                if structured_page._slot_deleted(slot_num) == False:
                    raw = structured_page.read_slot(slot_num)
                    record = self.deserialize(raw)
                    records.append(record)
            page = self.page_allocator.get_page(structured_page.next_page_id)
            structured_page = StructuredDataRecordPage(page.data)
        
        for slot_num in range(structured_page.num_slots):
            if structured_page._slot_deleted(slot_num) == False:
                raw = structured_page.read_slot(slot_num)
                record = self.deserialize(raw)
                records.append(record)

        return records


    def deserialize(self, data):
        """Convert bytes back into a Python dict according to the schema."""
        record = {}
        offset = 0

        for name, ftype in self.schema.fields:
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

    def delete(self, predicate):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        
        while structured_page.next_page_id != -1:
            for slot_num in range(structured_page.num_slots):
                if structured_page._slot_deleted(slot_num) == False:
                    raw = structured_page.read_slot(slot_num)
                    
                    # if raw is None:
                    #     print()
                        
                    record = self.deserialize(raw)
                
                    if predicate(record):
                        structured_page.delete_slot(slot_num)
            
            self.page_allocator.page_manager.write_page(page.id, structured_page.data)
            
            page = self.page_allocator.get_page(structured_page.next_page_id)
            structured_page = StructuredDataRecordPage(page.data)
        
        for slot_num in range(structured_page.num_slots):
            if structured_page._slot_deleted(slot_num) == False:
                raw = structured_page.read_slot(slot_num)
                
                # if raw is None:
                #         print()
                        
                record = self.deserialize(raw)
                
                if predicate(record):
                    structured_page.delete_slot(slot_num)

        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        self.page_allocator.page_manager.flush()
        
        
    def update(self, predicate, new_values: dict):
        page = self.page_allocator.get_page(self.first_page_id)
        structured_page = StructuredDataRecordPage(page.data)
        
        while structured_page.next_page_id != -1:
            for slot_num in range(structured_page.num_slots):
                if structured_page._slot_deleted(slot_num) == False:
                    raw = structured_page.read_slot(slot_num)
                        
                    record = self.deserialize(raw)
                
                    if predicate(record):
                        record.update(new_values)
                        serialized = structured_page.serialize(self.schema, record)
                        structured_page.update_slot(slot_num, serialized)
            
            self.page_allocator.page_manager.write_page(page.id, structured_page.data)
            
            page = self.page_allocator.get_page(structured_page.next_page_id)
            structured_page = StructuredDataRecordPage(page.data)
        
        for slot_num in range(structured_page.num_slots):
            if structured_page._slot_deleted(slot_num) == False:
                raw = structured_page.read_slot(slot_num)
                
                record = self.deserialize(raw)
                
                if predicate(record):
                    record.update(new_values)
                    serialized = structured_page.serialize(self.schema, record)
                    structured_page.update_slot(slot_num, serialized)

        self.page_allocator.page_manager.write_page(page.id, structured_page.data)
        self.page_allocator.page_manager.flush()
