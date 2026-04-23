from src.records.structured_records import Schema
from src.datatypes.classes import DataType

SYSTEM_TABLES_SCHEMA = Schema([
    ('table_id', DataType.INTEGER, 30),
    ('table_name', DataType.STRING, 30),
    ('first_page_id', DataType.INTEGER, 30),
])

SYSTEM_COLUMNS_SCHEMA = Schema([
    ('table_id', DataType.INTEGER, 30),
    ('column_id', DataType.INTEGER, 30),
    ('table_name', DataType.STRING, 30),
    ('column_name', DataType.STRING, 30),
    ('data_type', DataType.STRING, 30),
    ('data_length', DataType.INTEGER, 30),
    ('ordinal_position', DataType.INTEGER, 30),
])
    
SYSTEM_INDEXES_SCHEMA = Schema([
    ('index_id', DataType.INTEGER, 30),
    ('name', DataType.STRING, 30),
    ('table_id', DataType.INTEGER, 30),
    ('table_name', DataType.STRING, 30),
    ('column_name', DataType.STRING, 30),
    ('root_page_id', DataType.INTEGER, 30),
    ('unique', DataType.BOOLEAN, 1)
])
    
SYSTEM_SEQUENCES_SCHEMA = Schema([
    ('sequence_id', DataType.INTEGER, 30),
    ('name', DataType.STRING, 30),
    ('current_value', DataType.INTEGER, 30),
    ('increment', DataType.INTEGER, 30),
    ('min_value', DataType.INTEGER, 30),
    ('cache', DataType.INTEGER, 30),
    ('cycle', DataType.BOOLEAN, 1)
])