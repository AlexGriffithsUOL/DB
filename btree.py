from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType
from pathlib import Path
from src.indices.btree import BTreeNode as btn, BTreeIndex as bert

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
tm.create_index('idx_test_id', 'test', 'test_id', False)
tm.tables['system_tables'].range_scan('table_id', 0, 1000)
tm.tables['system_columns'].scan('table_id', 6)

for i in range(1000):
    test_record = {
        'test_id': i,
        'name': 'Alex',
        'ordinal': i
    }

    tm.insert('test', test_record)
    
# tm.sequence_manager._override_sequence_start_point('seq_test_sequence', 100)
tm.sequence_manager.create_sequence('test_sequence', 20, 20, 20, 'False')
sequence = tm.sequence_manager.generate_sequence_object('test_sequence')
    
for i in range(5849):
    # id = 5999 - i 
    id = i 
    if id == 5954:
        print('for fuck sake')
    tm.tables['test'].indexes['test_id'].delete(id)

print(tm.tables['test'].scan('test_id', 5999))
print(tm.tables['test'].scan('test_id', 5900))
print(tm.tables['test'].scan('test_id', 5850))

pass