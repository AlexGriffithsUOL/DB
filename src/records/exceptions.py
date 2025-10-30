from ..exceptions import BaseDBException

class BaseRecordManagerException(BaseDBException):
    prefix = 'RM'
    code = '000'
    msg = 'BASE RECORD MANAGER EXCEPTION'
    
class BaseDataRecordException(BaseDBException):
    prefix = 'DR'
    code = '000'
    msg = 'BASE DATA RECORD EXCEPTION'
    
class DataRecordNumSlotsNotIntException(BaseDataRecordException):
    code = '001'
    msg = 'Header value num_slots must be an integer value'
    
class DataRecordNumSlotsTooLongException(BaseDataRecordException):
    code = '002'
    msg = 'Header value num_slots length cannot exceed 2 bytes'
    
class DataRecordNotEnoughFreeSpaceException(BaseDataRecordException):
    code = '003'
    msg = 'Cannot insert record into page, size {data_length} bytes exceeds free space {free_space_available} bytes'
    
    def __init__(self, data_length, free_space_available):
        self.msg = self.msg.format(data_length=data_length, free_space_available=free_space_available)
        super().__init__()
        
class DataRecordSlotDoesNotExistException(BaseDataRecordException):
    code = '004'
    msg = 'Page does not contain slot {slot_number}'
    
    def __init__(self, slot_number):
        self.msg = self.msg.format(slot_number=slot_number)
        super().__init__()