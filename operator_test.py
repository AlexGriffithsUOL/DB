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

school_schema = Schema([
    ('school_id', DataType.INTEGER, 30),
    ('display_name', DataType.STRING, 30)
])

student_schema = Schema([
    ('student_id', DataType.INTEGER, 30),
    ('forename', DataType.STRING, 30),
    ('surname', DataType.STRING, 30),
    ('school_id', DataType.INTEGER, 30)
])

tm.create_table('school', school_schema)
tm.create_table('student', student_schema)

tm.sequence_manager.create_sequence('seq_school_id', 1, 1, 1, 1000, 'False')
tm.sequence_manager.create_sequence('seq_student_id', 1, 1, 1, 1000, 'False')

tx = tx_m.get_new_transaction()
school_id_sequence = tm.sequence_manager.get_sequence('seq_school_id', tx.start_snapshot)
student_id_sequence = tm.sequence_manager.get_sequence('seq_student_id', tx.start_snapshot)

tm.create_index('idx_school_id', 'school', 'school_id', False)
tm.create_index('idx_student_school_id', 'student', 'school_id', False)

stantonbury_school_id = school_id_sequence.nextval()
stantonbury_record = {
    'school_id': stantonbury_school_id,
    'display_name': 'stantonbury'
}

radcliffe_school_id = school_id_sequence.nextval()
radcliffe_record = {
    'school_id': radcliffe_school_id,
    'display_name': 'radcliffe'
}

tm.insert('school', stantonbury_record, tx)
tm.insert('school', radcliffe_record, tx)
    
tx.commit()

tx_student = tx_m.get_new_transaction()

first_student_id = student_id_sequence.nextval()
first_student_record = {
    'student_id': first_student_id,
    'forename': 'Zeeshan',
    'surname': 'Ali',
    'school_id': stantonbury_school_id
}
tm.insert('student', first_student_record, tx_student)

tx_student.commit()

tx7 = tx_m.get_new_transaction()

def join_pred(left, right):
    return left["school_id"] == right["school_id"]

ts = TableScan(tm.tables['student'], tx7)
filter = Filter(ts, lambda x: x['school_id'] > 0)
# projection = Project(filter, ['student_id', 'forename', 'surname'])
sort = MultiSort(filter, [('student_id', False)])

ts2 = TableScan(tm.tables['school'], tx7)

nlj = NestedLoopJoin(sort, ts2, join_pred)
lim = Limit(nlj, 1)


lim.open()
row = lim.next()
while row:
    print(row)
    row = lim.next()

lim.close()


# sort.open()
# row = sort.next()
# while row is not None:
#     print(row)
#     row = sort.next()
# sort.close()




# ts2 = TableScan(tm.tables['test'], tx7)
# index2 = tm.tables['test'].index_scan('test_id', 100, tx7)
# projection2 = Project(index2, ['name', 'ordinal'])

# def join_pred(left, right):
#     return left["ordinal"] == right["ordinal"]

# nlj = NestedLoopJoin(projection, projection2, join_pred)

# nlj.open()
# row = nlj.next()
# while row:
#     print(row)
#     row = nlj.next()

# nlj.close()