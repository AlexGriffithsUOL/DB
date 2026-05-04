from src.table_manager.manager import StorageEngine
from src.records.structured_records import Schema, DataType
from pathlib import Path
from src.transactions.manager import get_transaction_manager
import shutil
from src.server.classes import Server
from src.operators.classes import TableScan, Filter, Project, NestedLoopJoin, MultiSort, Limit, Aggregate, IndexScan
from src.parser.parser import Parser
from src.parser.tokeniser import Tokeniser
from src.parser.executor import Executor
import random

file_path = Path("/home/alex/Programming/DBMS/pysql.db")
sequence_dir_path = Path("/home/alex/Programming/DBMS/internal/")

if file_path.exists():
    file_path.unlink()
    
if sequence_dir_path.exists():
    shutil.rmtree(sequence_dir_path)

tm = StorageEngine()



# tx = tm.transaction_manager.get_new_transaction()

# ts = TableScan(tm.tables['system_tables'], tx)
# proj = Project(ts, ['table_id', 'table_name'])
# srt = MultiSort(proj, [('table_id', True)])
# proj2 = Project(srt, ['table_name'])

# proj2.open()
# row = proj2.next()
# while row:
#     print(row)
#     row = proj2.next()
# proj2.close()
