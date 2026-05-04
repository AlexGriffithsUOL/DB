from src.table_manager.manager import StorageEngine
from src.records.structured_records import Schema, DataType
from pathlib import Path
from src.transactions.manager import get_transaction_manager
import shutil
from src.server.classes import Server
from src.operators.classes import TableScan, Filter, Project, NestedLoopJoin, MultiSort, Limit, Aggregate, IndexScan
import random
from src.parser.parser import Parser
from src.parser.tokeniser import Tokeniser
from src.parser.executor import Executor
from src.parser.planner import PlanBuilder

first_names = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona"]
surnames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller"]

def generate_random_name():
    first_name = random.choice(first_names)
    surname = random.choice(surnames)
    return first_name, surname

file_path = Path("/home/alex/Programming/DBMS/pysql.db")
sequence_dir_path = Path("/home/alex/Programming/DBMS/internal/")

if file_path.exists():
    file_path.unlink()
    
if sequence_dir_path.exists():
    shutil.rmtree(sequence_dir_path)

tx_m = get_transaction_manager()
tm = StorageEngine(transaction_manager=tx_m)

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
tm.create_index('idx_student_id', 'student', 'student_id', False)
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

def generate_student():
    student_id = student_id_sequence.nextval()
    forename, surname = generate_random_name()
    
    return {
        'student_id': student_id,
        'forename': forename,
        'surname': surname   
    }

def generate_stantonbury_student():
    student = generate_student()
    student['school_id'] = stantonbury_school_id
    return student
    
def generate_radcliffe_student():
    student = generate_student()
    student['school_id'] = radcliffe_school_id
    return student
    
tm.insert('student', first_student_record, tx_student)

for i in range(6):
    student = generate_stantonbury_student()
    tm.insert('student', student, tx_student)
    
for i in range(3):
    student = generate_radcliffe_student()
    tm.insert('student', student, tx_student)

tx_student.commit()




planner = PlanBuilder(tm)

while True:
    try:
        tx = tx_m.get_new_transaction()
        select = input('write select: ')
        
        if select == 'exit':
            print('Exiting')
            break
        
        tokeniser = Tokeniser(select)
        tokens = tokeniser.tokenise()
        parser = Parser(tokens)
        ast = parser.parse_select()
        plan = planner.build(ast, tx)
        
        plan.open()
        row = plan.next()
        while row:
            print(row)
            row = plan.next()
        plan.close()
        tx.commit()
    except Exception as err:
        tx.rollback()
        print(err)