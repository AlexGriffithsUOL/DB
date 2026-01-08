from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType
from src.indices.btree import BTreeIndex, BTreeNode
from pathlib import Path

file_path = Path("/home/alex/Programming/DBMS/pysql.db")

if file_path.exists():
    file_path.unlink()

tm = TableManager()

test_table_schema = Schema([
    ('test_id', DataType.INTEGER),
    ('name', DataType.STRING),
    ('ordinal', DataType.INTEGER)
])

tm.create_table('test', test_table_schema)
bti = tm.create_index('idx_test_id', 'test', 'test_id', False)

def get_name(num):
    string = 'owugabooga'[:1+(num%len('owugabooga'))]
    return string

record_list = []
bti_list = []

for i in range(3000):
    record = {
        'test_id': i+1,
        'name': get_name(i),
        'ordinal': i
    }
    
    page_id, slot_id = tm.insert('test', record)
    
    record_list.append(record)
    bti_list.append((record['test_id'], (page_id, slot_id)))
    
    # if i == 234:
    #     print('catch 234')
    #     bti.insert(record['test_id'], (page_id, slot_id)) # needs to be able to handle fcking integers and strings
    #     continue
    if i == 235:
        print('catch 235')
        bti.insert(record['test_id'], (page_id, slot_id)) # needs to be able to handle fcking integers and strings
        continue
    
    if i == 380 or i == 379: # appears to not be saving on 379
        print(f'catch {i}')
        bti.insert(record['test_id'], (page_id, slot_id)) # needs to be able to handle fcking integers and strings
        continue
    
    # if i == 793:
    #     print('catch 793')
    # if i == 794:
    #     print('catch 794')
    
    bti.insert(record['test_id'], (page_id, slot_id)) # needs to be able to handle fcking integers and strings
    
print(bti.search(0))
print(bti.search(1))
print(tm.table_column_has_index('test', 'test_id'))