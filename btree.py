from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType
from pathlib import Path
from src.transactions.manager import get_transaction_manager
import shutil
from src.server.classes import Server
from src.operators.classes import TableScan, Filter, Project, NestedLoopJoin, MultiSort, Limit


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