from src.wal.classes import WriteAheadLogger
import pathlib

path = '/home/alex/Programming/DBMS/internal/$wal/' + 'wal.wal'

try:
    log_path = pathlib.Path(path).unlink()
except:
    pass

wal = WriteAheadLogger(path)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
wal.write(1, 'DELETE', 'test', (22, 121), {'test_id': 1000, 'name': 'Alex', 'ordinal': 9999}, None, 'idx_test_id', 1000)
e = wal.content
pass