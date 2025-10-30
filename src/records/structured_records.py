from .records import DataRecordPage
from .exceptions import DataRecordSlotDoesNotExistException
from src.config import ENDIAN_TYPE

class DataType:
    STRING = 'string'
    INTEGER = 'int'
    BOOLEAN = 'bool'

class Schema:
    def __init__(self, fields: list[tuple[str, str]]):
        self.fields = fields

    def get_field_names(self):
        return [name for name, _ in self.fields]

class StructuredDataRecordPage(DataRecordPage):
    def serialize(self, schema: Schema, record: dict) -> bytes:
        data = bytearray()
        for name, ftype in schema.fields:
            if name not in record:
                raise ValueError(f"Missing value for field '{name}'")
            value = record[name]

            match(ftype):
                case DataType.INTEGER:
                    if not isinstance(value, int):
                        raise TypeError(f"Expected int for field '{name}', got {type(value)}")
                    data += value.to_bytes(4, byteorder=ENDIAN_TYPE, signed=True)

                case (DataType.STRING):
                    if not isinstance(value, str):
                        raise TypeError(f"Expected str for field '{name}', got {type(value)}")
                    encoded = value.encode('utf-8')
                    length = len(encoded)
                    if length > 65535:
                        raise ValueError(f"String too long for field '{name}'")
                    data += length.to_bytes(2, byteorder=ENDIAN_TYPE)  # 2-byte length prefix
                    data += encoded
                
                case (DataType.BOOLEAN):
                    if not isinstance(value, str):
                        raise TypeError(f"Expected str for field '{name}', got {type(value)}")
                    
                    if value not in ('True', 'False'):
                        raise TypeError(f"Expected str for field in True, False, got {value}")
                
                    encoded = value.encode('utf-8')
                    length = len(encoded)
                    if length > 65535:
                        raise ValueError(f"String too long for field '{name}'")
                    data += length.to_bytes(2, byteorder=ENDIAN_TYPE)  # 2-byte length prefix
                    data += encoded

                case _:
                    raise ValueError(f"Unsupported data type: {ftype}")

        return bytes(data)
    
    def deserialize(self, schema: Schema, data: bytes) -> dict:
        """Convert bytes back into a Python dict according to the schema."""
        record = {}
        offset = 0

        for name, ftype in schema.fields:
            match (ftype):
                case (DataType.INTEGER):
                    if offset + 4 > len(data):
                        raise ValueError(f"Not enough bytes for INTEGER field '{name}'")
                    value = int.from_bytes(data[offset:offset + 4], byteorder=ENDIAN_TYPE, signed=True)
                    record[name] = value
                    offset += 4

                case DataType.STRING:
                    if offset + 2 > len(data):
                        raise ValueError(f"Not enough bytes for STRING length in field '{name}'")
                    length = int.from_bytes(data[offset:offset + 2], byteorder=ENDIAN_TYPE)
                    offset += 2
                    if offset + length > len(data):
                        raise ValueError(f"Not enough bytes for STRING data in field '{name}'")
                    encoded = data[offset:offset + length]
                    record[name] = encoded.decode('utf-8')
                    offset += length

                case _:
                    raise ValueError(f"Unsupported data type: {ftype}")
            
        return record
    
    def insert_record(self, schema: Schema, record: dict) -> int:
        """Insert a structured record into the page."""
        serialized = self.serialize(schema, record)
        slot_id = self.insert(serialized)
        return slot_id

    def read_record(self, schema: Schema, slot_number: int) -> dict:
        """Read a structured record by slot ID."""
        raw = self.read_slot(slot_number)
        if raw is None:
            raise DataRecordSlotDoesNotExistException(slot_number)
        return self.deserialize(schema, raw)

    def update_record(self, schema: Schema, slot_number: int, record: dict):
        """Update an existing record in-place or move it if needed."""
        serialized = self.serialize(schema, record)
        self.update_slot(slot_number, serialized)
