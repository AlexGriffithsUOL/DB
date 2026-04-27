from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType
from pathlib import Path
from src.transactions.manager import get_transaction_manager
import shutil


file_path = Path("/home/alex/Programming/DBMS/pysql.db")
sequence_dir_path = Path("/home/alex/Programming/DBMS/internal/")

if file_path.exists():
    file_path.unlink()
    
if sequence_dir_path.exists():
    shutil.rmtree(sequence_dir_path)

tx_m = get_transaction_manager()
tm = TableManager(transaction_manager=tx_m)

test_table_schema = Schema([
    ('test_id', DataType.INTEGER, 30),
    ('name', DataType.STRING, 30),
    ('ordinal', DataType.INTEGER, 30)
])

tm.create_table('test', test_table_schema)#, tx.id, tx.start_snapshot) ## need to add tx_id, and tx_snapshot

tm.sequence_manager.create_sequence('test_sequence', 1, 1, 1, 1000, 'False')

tx = tx_m.get_new_transaction()
sequence = tm.sequence_manager.get_sequence('test_sequence', tx.start_snapshot)

tm.create_index('idx_test_id', 'test', 'test_id', False)

for i in range(3000):
        
    fancy_id = sequence.nextval()
    test_record = {
        'test_id': fancy_id,
        'name': 'Alex',
        'ordinal': fancy_id
    }

    tm.insert('test', test_record, tx)
    
tx.commit()


tx_a_test = tx_m.get_new_transaction()
test_record = {
    'test_id': 50000,
    'name': 'AlexTest',
    'ordinal': 8008135
}
tm.insert('test', test_record,  tx_a_test)
tx_a_test.rollback()


tx2 = tx_m.get_new_transaction()
tm.tables['test'].update(lambda x: x['test_id'] == 1000, {'ordinal': 9999}, tx2, 'test_id', 1000 )
tx2.commit()

tx3 = tx_m.get_new_transaction()
tm.tables['test'].update(lambda x: x['test_id'] == 1000, {'ordinal': 12}, tx3, 'test_id', 1000 )
tx3.rollback()

# tx11 = tx_m.get_new_transaction()
# tm.tables['test']

tx4 = tx_m.get_new_transaction()
records2 = tm.tables['test'].scan(lambda x: x['test_id'] == 1000, tx4.start_snapshot, index_column = 'test_id', index_value = 1000)
tx4.commit()

tx5 = tx_m.get_new_transaction()
tm.tables['test'].delete(lambda x: x['test_id'] == 1000, tx5, 'test_id', 1000)
tx5.rollback()

tx10 = tx_m.get_new_transaction()
records2 = tm.tables['test'].scan(lambda x: x['test_id'] == 1000, tx4.start_snapshot, index_column = 'test_id', index_value = 1000)
tx10.commit()

tx6 = tx_m.get_new_transaction()
records2 = tm.tables['test'].scan(lambda x: x['test_id'] == 1000, tx6.start_snapshot, index_column = 'test_id', index_value = 1000)
tx6.commit()
# tx6.commit()

tm.create_index('idx_system_tables_table_name', 'system_tables', 'table_name', True)