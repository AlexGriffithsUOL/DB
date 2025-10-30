from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType

tm = TableManager()

test_table_schema = Schema([
    ('test_id', DataType.INTEGER),
    ('name', DataType.STRING)
])

tm.create_table('test', test_table_schema)
tm.create_table('test_two', test_table_schema)

for i in range(100):
    record = {
        'test_id': i,
        'name': 'owugabooga'
    }
    
    tm.tables['test'].insert(record)

tptp = tm.tables['test'].scan_all_records()

tm.tables['test'].delete(lambda rec: rec['test_id'] % 2 == 0)
tm.tables['test'].update(lambda rec: rec['test_id'] % 3 == 0, {'name': 'fizz'})
tm.tables['test'].update(lambda rec: rec['test_id'] % 5 == 0, {'name': 'buzz'})
    
tptp2 = tm.tables['test'].scan_all_records()

tm.tables['test'].delete(lambda rec: rec['test_id'] is not None)

tptp3 = tm.tables['test'].scan_all_records()
pass