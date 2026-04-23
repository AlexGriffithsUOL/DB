#NOTE: THiS IS GOING TO GO <TARGET_TABLE>_<INSERTED_RECORD_NAME>_RECORD
from src.datatypes.classes import DataType
from src.table_manager.seed.core.utils import BaseSeedData

class SystemTablesSeedData(BaseSeedData):
    SYSTEM_TABLES_SYSTEM_TABLES_RECORD: dict = {'table_id': 1, 'table_name': 'system_tables', 'first_page_id': 2}
    SYSTEM_TABLES_SYSTEM_COLUMNS_RECORD: dict = {'table_id': 2, 'table_name': 'system_columns', 'first_page_id': 3}
    
    