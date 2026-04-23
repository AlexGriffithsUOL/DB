from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType
from pathlib import Path

file_path = Path("/home/alex/Programming/DBMS/pysql.db")

if file_path.exists():
    file_path.unlink()

tm = TableManager()

test_table_schema = Schema([
    ('test_id', DataType.INTEGER, 30),
    ('name', DataType.STRING, 30),
    ('ordinal', DataType.INTEGER, 30)
])

tm.create_table('test', test_table_schema)

tm.sequence_manager.create_sequence('test_sequence', 1, 1, 1, 1000, 'False')
sequence = tm.sequence_manager.generate_sequence_object('test_sequence')

for i in range(3000):
        
    fancy_id = sequence.nextval()
    test_record = {
        'test_id': fancy_id,
        'name': 'Alex',
        'ordinal': fancy_id
    }

    tm.insert('test', test_record)
    
tm.create_index('idx_test_id', 'test', 'test_id', False)

for i in range(1, 2000):
    # id = 5999 - i 
    # id = (i+ 1) * 20
    id = i
    
    tm.tables['test'].indexes['test_id'].delete(id)


tm.create_index('idx_system_tables_table_name', 'system_tables', 'table_name', True)
pass