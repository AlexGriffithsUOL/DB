from .exceptions import *
from src.config import ENDIAN_TYPE, NULL_PAGE_ID_VALUE

class SlotHelper:
    def __init__(self, offset, length):
        self.offset = int.from_bytes(offset, ENDIAN_TYPE)
        self.length = int.from_bytes(length, ENDIAN_TYPE, signed=True)

class DataRecordPage:
    RESERVED_HEADER_SIZE_BYTES = 16
    
    NUM_SLOTS_START_PTR = 0
    NUM_SLOTS_END_PTR = 2
    
    NEXT_PAGE_ID_START_PTR = 2
    NEXT_PAGE_ID_END_PTR = 10
    
    FREE_SPACE_OFFSET_START_PTR = 10
    FREE_SPACE_OFFSET_END_PTR = 14
    
    FREE_SPACE_SIZE_START_PTR = 14
    FREE_SPACE_SIZE_END_PTR = 16
    
    SLOT_SIZE_BYTES = 4
    
    def __init__(self, data, file_length=4096):
        self.data = data
        self.file_length = file_length
        
        if self.uninitialised_page:
            self.init()
    
    def init(self):
        self.init_empty_data()
        self.num_slots = 0
        self.next_page_id = NULL_PAGE_ID_VALUE
        self.free_space_offset = self.file_length
        self.free_space_size = self.file_length - self.RESERVED_HEADER_SIZE_BYTES
        
    def init_empty_data(self):
        self.data = bytearray(b''.ljust(self.file_length, b'\x00'))
    
    @property
    def uninitialised_page(self):
        return all(b == 0 for b in self.data)
    
    @property
    def num_slots(self):
        return int.from_bytes(
            self.data[self.NUM_SLOTS_START_PTR:self.NUM_SLOTS_END_PTR],
            ENDIAN_TYPE
        )
    
    @property
    def next_page_id(self):
        return int.from_bytes(
            self.data[self.NEXT_PAGE_ID_START_PTR:self.NEXT_PAGE_ID_END_PTR],
            ENDIAN_TYPE,
            signed=True
        )
    
    @property 
    def free_space_offset(self):
        return int.from_bytes(
            self.data[self.FREE_SPACE_OFFSET_START_PTR:self.FREE_SPACE_OFFSET_END_PTR],
            ENDIAN_TYPE
        )
    
    @property
    def free_space_size(self):
        return int.from_bytes(
            self.data[self.FREE_SPACE_SIZE_START_PTR:self.FREE_SPACE_SIZE_END_PTR], 
            ENDIAN_TYPE
        )
    
    @property
    def slots(self):
        slot_directory_start = self.RESERVED_HEADER_SIZE_BYTES
        slot_directory_end = slot_directory_start + (self.num_slots * self.SLOT_SIZE_BYTES)
        
        slots = []
        for i in range(slot_directory_start, slot_directory_end, self.SLOT_SIZE_BYTES):
            slot = self.data[i: i + self.SLOT_SIZE_BYTES]
            slots.append(SlotHelper(slot[0:2], slot[2:4]))
            
        return slots

    
    def _set_in_header(self, value, start, end):
        size = end -start
        try:
            value = int(value)
        except:
            raise DataRecordNumSlotsNotIntException()
        
        byte_value = int.to_bytes(value, size, ENDIAN_TYPE, signed=True)
        
        if len(byte_value) > size:
            raise DataRecordNumSlotsTooLongException()
        
        self.data[start:end] = byte_value

    @num_slots.setter
    def num_slots(self, val):
        self._set_in_header(
            val,
            self.NUM_SLOTS_START_PTR,
            self.NUM_SLOTS_END_PTR
        )
        
    @free_space_offset.setter
    def free_space_offset(self, val):
        self._set_in_header(
            val,
            self.FREE_SPACE_OFFSET_START_PTR,
            self.FREE_SPACE_OFFSET_END_PTR
        )
    
    @free_space_size.setter
    def free_space_size(self, val):
        self._set_in_header(
            val,
            self.FREE_SPACE_SIZE_START_PTR,
            self.FREE_SPACE_SIZE_END_PTR
        )
        
    @next_page_id.setter
    def next_page_id(self, val):
        self._set_in_header(
            val,
            self.NEXT_PAGE_ID_START_PTR,
            self.NEXT_PAGE_ID_END_PTR
        )
        
    @property
    def slot_directory_end(self):
        return (self.RESERVED_HEADER_SIZE_BYTES) + (self.num_slots * self.SLOT_SIZE_BYTES)
        
    @property
    def full(self):
        return self.slot_directory_end == self.free_space_offset
    
    def _slot_deleted(self, slot_number):
        if self._slot_exists(slot_number):
            start = (self.RESERVED_HEADER_SIZE_BYTES + (slot_number * self.SLOT_SIZE_BYTES) + 2)
            return int.from_bytes(self.data[start:start+2], ENDIAN_TYPE, signed=True) == -1
        
    def _get_slot_info(self, slot_number):
        if slot_number < self.num_slots and slot_number >= 0:
            slot_dir_start, slot_dir_end = self._slot_start_and_end(slot_number)
            
            slot_offset = self.data[slot_dir_start : slot_dir_start + 2]
            slot_length = self.data[slot_dir_start+2 : slot_dir_end]
            
            slot_offset = int.from_bytes(slot_offset, ENDIAN_TYPE)
            slot_length = int.from_bytes(slot_length, ENDIAN_TYPE, signed=True)
            return slot_offset, slot_length
        else:
            raise DataRecordSlotDoesNotExistException(slot_number)
    
    def _read_slot(self, start, end):
        slot_offset = self.data[start:(start + 2)]
        slot_length = self.data[(end - 2): end]
        return slot_offset, slot_length
    
    def delete_slot(self, slot_number):
        if self._slot_exists(slot_number) and not self._slot_deleted(slot_number):
            slot_dir_start, slot_dir_end = self._slot_start_and_end(slot_number)
            freed_space = int.from_bytes(self.data[slot_dir_start+2:slot_dir_end], ENDIAN_TYPE, signed=True)
            self.free_space_size += freed_space
            self.data[slot_dir_start+2 : slot_dir_end] = int.to_bytes(-1, 2, ENDIAN_TYPE, signed=True)
            
        else:
            raise DataRecordSlotDoesNotExistException(slot_number)
        
    def _slot_exists(self, slot_number):
        return (slot_number < self.num_slots and slot_number >= 0)
    
    def _slot_start_and_end(self, slot_number):
        start  = (self.RESERVED_HEADER_SIZE_BYTES + (slot_number * self.SLOT_SIZE_BYTES))
        end = start + self.SLOT_SIZE_BYTES
        return start, end
    
    def _raw_write_to_slot(self, slot_number, offset, length):
        if self._slot_exists(slot_number):
            slot_dir_start, slot_dir_end = self._slot_start_and_end(slot_number)
            self.data[slot_dir_start:slot_dir_start+2] = int.to_bytes(offset, 2, ENDIAN_TYPE)
            self.data[slot_dir_start+2: slot_dir_end] = int.to_bytes(length, 2, ENDIAN_TYPE, signed=True)
            return slot_number
    
    def _safe_write_to_slot(self, slot_number, offset, length):
        if not self._slot_deleted(slot_number):
            return self._raw_write_to_slot(slot_number, offset, length)
        
    def _can_fit(self, data_length):
        return self.free_space_size >= data_length
    
    def update_slot(self, slot_number, data):
        if self._slot_exists(slot_number) and not self._slot_deleted(slot_number):
            slot: SlotHelper = self.slots[slot_number]
            old_length = slot.length
            new_length = len(data)
            if new_length <= old_length:
                start = slot.offset
                self.data[start : start + new_length] = data

                if new_length < old_length:
                    self.data[start + new_length : start + old_length] = b'\x00' * (old_length - new_length)

                slot.length = new_length
                self._safe_write_to_slot(slot_number, slot.offset, slot.length)

            else:
                if not self._can_fit(new_length):
                    raise MemoryError("Not enough space in page to update record")

                new_offset = self.free_space_offset
                self.data[new_offset : new_offset + new_length] = data

                slot.offset = new_offset
                slot.length = new_length

                self._safe_write_to_slot(slot_number, slot.offset, slot.length)
                self.free_space_offset = new_offset
                self.free_space_size -= new_length

                # (Old space remains unused — will be compacted later if needed)
    
    def _add_slot(self, slot_offset, slot_length):
        slot_start = self.slot_directory_end
        
        self.data[slot_start : (slot_start + 2)] = int.to_bytes(slot_offset, 2, ENDIAN_TYPE)
        self.data[(slot_start + 2) : (slot_start + 4)] = int.to_bytes(slot_length, 2, ENDIAN_TYPE, signed=True)
        
        self.num_slots += 1
        
    def read_slot(self, slot_number):
        if not self._slot_deleted(slot_number) and self._slot_exists(slot_number):
            slot = self.slots[slot_number]
            return self.data[slot.offset:slot.offset + slot.length]
    
    def insert(self, data):
        data_length = len(data) 
        full_data_length = data_length + self.SLOT_SIZE_BYTES
        if full_data_length > self.free_space_size:
            raise DataRecordNotEnoughFreeSpaceException(full_data_length, self.free_space_size)
        
        self.free_space_offset -= data_length
        self.free_space_size -= full_data_length
        self.data[self.free_space_offset : self.free_space_offset + data_length] = data
        self._add_slot(self.free_space_offset, data_length)
        return self.num_slots - 1
        
    def compact(self):
        useful_data =  [x for x in self.slots if x.length > -1]
        
        offset_marker = 4096
        for i, piece in enumerate(useful_data):
            data = self.read_slot(i)
            offset_marker -= piece.length
            self.data[offset_marker:offset_marker + len(data)] = data
            self._safe_write_to_slot(i, offset_marker, piece.length)
            
        self.num_slots -= (self.num_slots - len(useful_data))
        self.free_space_offset = offset_marker
        self.free_space_size = self.free_space_offset - self.slot_directory_end
