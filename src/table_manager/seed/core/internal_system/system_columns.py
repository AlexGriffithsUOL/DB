from src.datatypes.classes import DataType
from src.table_manager.seed.core.utils import BaseSeedData

class SystemColumnsSeedData(BaseSeedData):
    SYSTEM_COLUMNS_SYSTEM_TABLES_RECORD: list[dict] = [
        {
            'table_id': 1,
            'column_id': 1,
            'table_name': 'system_tables',
            'column_name': 'table_id',
            'data_type': DataType.INTEGER,
            'data_length': 30,
            'ordinal_position': 1
        },
        {
            'table_id': 1,
            'column_id': 2,
            'table_name': 'system_tables',
            'column_name': 'table_name',
            'data_type': DataType.STRING,
            'data_length': 30,
            'ordinal_position': 2
        },
        {
            'table_id': 1,
            'column_id': 3,
            'table_name': 'system_tables',
            'column_name': 'first_page_id',
            'data_type': DataType.INTEGER,
            'data_length': 30,
            'ordinal_position': 3
        }
    ]
    
    SYSTEM_COLUMNS_SYSTEM_COLUMNS_RECORD = [
        {
            'table_id': 2,
            'column_id': 1,
            'table_name': 'system_columns',
            'column_name': 'table_id',
            'data_type': DataType.INTEGER,
            'data_length': 30,
            'ordinal_position': 1
        },
        {
            'table_id': 2,
            'column_id': 2,
            'table_name': 'system_columns',
            'column_name': 'column_id',
            'data_type': DataType.INTEGER,
            'data_length': 30,
            'ordinal_position': 1
        },
        {
            'table_id': 2,
            'column_id': 3,
            'table_name': 'system_columns',
            'column_name': 'table_name',
            'data_type': DataType.STRING,
            'data_length': 30,
            'ordinal_position': 2
        },
        {
            'table_id': 2,
            'column_id': 4,
            'table_name': 'system_columns',
            'column_name': 'column_name',
            'data_type': DataType.STRING,
            'data_length': 30,
            'ordinal_position': 2
        },
        {
            'table_id': 2,
            'column_id': 3,
            'table_name': 'system_columns',
            'column_name': 'data_type',
            'data_type': DataType.STRING,
            'data_length': 30,
            'ordinal_position': 3
        },
        {
            'table_id': 2,
            'column_id': 4,
            'table_name': 'system_columns',
            'column_name': 'data_length',
            'data_type': DataType.INTEGER,
            'data_length': 30,
            'ordinal_position': 3
        },
        {
            'table_id': 2,
            'column_id': 5,
            'table_name': 'system_columns',
            'column_name': 'ordinal_position',
            'data_type': DataType.INTEGER,
            'data_length': 30,
            'ordinal_position': 4
        }
    ]