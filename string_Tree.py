from src.table_manager.manager import TableManager
from src.records.structured_records import Schema, DataType
from pathlib import Path
import random
import string

def generate_unique_strings(n, min_len=5, max_len=20):
    """
    Generate `n` unique variable-length strings.
    Each string has length between min_len and max_len.
    """
    unique_strings = set()
    
    while len(unique_strings) < n:
        length = random.randint(min_len, max_len)
        # Generate a random string of the given length
        s = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        unique_strings.add(s)
    
    return list(unique_strings)

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

tm.create_index('idx_test_name', 'test', 'name', True)

# strings = generate_unique_strings(3000, 4, 25)
# new_strings = [f'{x}\n' for x in strings]
# file = open('test.txt', 'w+')
# file.writelines(new_strings)
# file.flush()
# file.close()
file = open('test.txt', 'r')
content = file.readlines()
content = [x[:-2] for x in content]
file.close()
strings = content

for i in range(3000):
        
    fancy_id = sequence.nextval()
    test_record = {
        'test_id': fancy_id,
        'name': strings[i],
        'ordinal': fancy_id
    }
    
    if i in (269, 270, 271):
        print('checkpoint') # Fix this, seems to be some weird shrinking when its the bloody promotion

    tm.insert('test', test_record)
    
# tm.create_index('idx_system_tables_name', 'system_tables', 'table_name', unique = True)

print(tm.tables['test'].scan('name', 'HEz3UByeduxVHfGDSJRoRkk'))

pass