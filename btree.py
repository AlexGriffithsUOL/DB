from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType
from src.indices.tes import BTreeIndex, BTreeNode

tm = TableManager()
bti = BTreeIndex(tm.page_allocator)

test_table_schema = Schema([
    ('test_id', DataType.INTEGER),
    ('name', DataType.STRING)
])

tm.create_table('test', test_table_schema)

tm.system_indexes.insert({
        'index_id': 1,
        'name': 'idx_test_id',
        'table_id': 8,
        'column_name': 'bame',
        'root_page_id': bti.root_page_id,
        'unique': 'False'
})

for i in range(2499):
    record = {
        'test_id': i+1,
        'name': 'owugabooga'
    }
    
    page_id, slot_id = tm.insert('test', record)
    bti.insert('owugabooga', (page_id, slot_id))
    
# print(bti.search(71))

pass