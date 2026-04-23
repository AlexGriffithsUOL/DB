from abc import ABC, abstractmethod

class DataType:
    STRING = 'S'
    INTEGER = 'I'
    BOOLEAN = 'B'
    
class DataTypeBaseType:
    reference_char = 'N'
    
    def __init__(self, length: int, *args, **kwargs):
        self.length = length
        
    def encode(self, value):
        raise NotImplementedError()
    
    def get_length(self, value):
        raise NotImplementedError()
    
    def serialise(self, value: str):
        encoded = self.encode(value)
        
        if self.get_length(value) > self.length:
            raise Exception('VALUE TOO BIG')
        
        return encoded
    
    def deserialise(self, values):
        pass

class StringDataType(DataTypeBaseType):
    reference_char = 'S'
    encoding = 'utf-8'
    
    def encode(self, value):
        length_bytes = int.to_bytes(len(value), length=2, byteorder='little', signed=False)
        return length_bytes + value.encode(self.encoding)
    
    def get_length(self, value):
        return len(value)
    
    def deserialise(self, values: bytearray):
        return values.decode('utf-8')
    
class IntegerDataType(DataTypeBaseType):
    reference_char = 'I'
    
    def __init__(self, length: int, signed: bool = True):
        
        if length < 4:
            raise Exception('Length too small')
        
        self.length = length
        self.signed = signed
        
    def get_length(self, value):
        return len(str(value))
        
    def encode(self, value):
        return int.to_bytes(value, length=self.length, byteorder='little', signed=True)
    
    def deserialise(self, values: list):
        return int.from_bytes(values, byteorder='little', signed=self.signed)
        

DATATYPE_MAP = {
    IntegerDataType.reference_char: IntegerDataType,
    StringDataType.reference_char: StringDataType
}

def get_datatype(datatype_code: str):
    return DATATYPE_MAP[datatype_code]