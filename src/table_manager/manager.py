from .table import Table
from src.config import ENDIAN_TYPE
from src.records.structured_records import DataType, Schema, StructuredDataRecordPage
from src.pages.allocator import PageAllocator
from src.catalog.header import CatalogHeader
from src.indices.tes import BTreeIndex

class TableManager:
    @property
    def _system_tables_schema_(self):
        return Schema([
            ('table_id', DataType.INTEGER),
            ('table_name', DataType.STRING),
            ('first_page_id', DataType.INTEGER),
        ])
  
    @property
    def _system_columns_schema_(self):      
        return Schema([
            ('table_id', DataType.INTEGER),
            ('column_name', DataType.STRING),
            ('data_type', DataType.STRING),
            ('ordinal_position', DataType.INTEGER),
        ])
        
    @property
    def _system_indexes_schema_(self):
        return Schema([
            ('index_id', DataType.INTEGER),
            ('name', DataType.STRING),
            ('table_id', DataType.INTEGER),
            ('column_name', DataType.STRING),
            ('root_page_id', DataType.INTEGER),
            ('unique', DataType.BOOLEAN)
        ])
        
    @property
    def _system_sequences_schema_(self):
        return Schema([
            ('sequence_id', DataType.INTEGER),
            ('name', DataType.STRING),
            ('current_value', DataType.INTEGER),
            ('increment', DataType.INTEGER),
            ('min_value', DataType.INTEGER),
            ('cycle', DataType.BOOLEAN)
        ])
        
    def _schema_to_sys_cols_(self, schema, table_id):
        columns = []
        
        for i, field_data in enumerate(schema.fields):
            columns.append(
                {
                    'table_id': table_id,
                    'column_name': field_data[0],
                    'data_type': field_data[1],
                    'ordinal_position': i + 1
                }
            )
            
        return columns
    
    def _get_table_id_counter_(self):
        self.catalog_header.table_counter += 1
        return self.catalog_header.table_counter
    
    def __init__(self, page_manager = None):
        
        if page_manager is None:
            page_manager = PageAllocator()

        self.page_allocator: PageAllocator = page_manager
        self.tables = {}
        self.catalog_header = CatalogHeader(self.page_allocator)
        
        self._table_id_counter_ = self.catalog_header.table_counter
        
        if self.page_allocator.fresh_superblock:
            self._initialise_system_catalog_()
            
        self.load_catalog()
            
    def _initialise_system_tables_(self):
        system_tables_sys_table_record = {'table_id': 1, 'table_name': 'system_tables', 'first_page_id': 2}
        system_columns_sys_table_record = {'table_id': 2, 'table_name': 'system_columns', 'first_page_id': 3}
        system_tables_page = StructuredDataRecordPage(self.page_allocator.get_page(2).data)
        
        system_tables_page.insert_record(
            schema = self._system_tables_schema_,
            record=system_tables_sys_table_record
        )
        
        system_tables_page.insert_record(
            schema=self._system_tables_schema_,
            record= system_columns_sys_table_record
        )
        
        self.page_allocator.page_manager.write_page(2, system_tables_page.data)
        self.page_allocator._mark_page_id(2)
        
    def _initialise_system_indexes_(self):
        self.system_indexes = self.create_table('system_indexes', self._system_indexes_schema_)
    
    def _initialise_system_sequences_(self):
        self.system_sequences = self.create_table('system_sequences', self._system_sequences_schema_)
        
    def _initialise_system_columns_(self):
        system_columns_page = StructuredDataRecordPage(self.page_allocator.get_page(3).data)
        
        system_tables_sys_columns_records = [
            {
                'table_id': 1,
                'column_name': 'table_id',
                'data_type': DataType.INTEGER,
                'ordinal_position': 1
            },
            {
                'table_id': 1,
                'column_name': 'table_name',
                'data_type': DataType.STRING,
                'ordinal_position': 2
            },
            {
                'table_id': 1,
                'column_name': 'first_page_id',
                'data_type': DataType.INTEGER,
                'ordinal_position': 3
            }
        ]

        system_columns_sys_columns_records = [
            {
                'table_id': 2,
                'column_name': 'table_id',
                'data_type': DataType.INTEGER,
                'ordinal_position': 1
            },
            {
                'table_id': 2,
                'column_name': 'column_name',
                'data_type': DataType.STRING,
                'ordinal_position': 2
            },
            {
                'table_id': 2,
                'column_name': 'data_type',
                'data_type': DataType.STRING,
                'ordinal_position': 3
            },
            {
                'table_id': 2,
                'column_name': 'ordinal_position',
                'data_type': DataType.INTEGER,
                'ordinal_position': 4
            }
        ]

        for i in system_tables_sys_columns_records:
            system_columns_page.insert_record(
                schema=self._system_columns_schema_,
                record = i
            )

        for i in system_columns_sys_columns_records:
            system_columns_page.insert_record(
                schema=self._system_columns_schema_,
                record = i
            )
            
        self.page_allocator.page_manager.write_page(3, system_columns_page.data)
        self.page_allocator._mark_page_id(3)
    
    def _initialise_metadata_indexes_(self):
        
        system_table_table_id_index = {
            'index_id' : 1,
            'name' : 'pk_system_tables_table_id',
            'table_id' : 1,
            'column_name' : 'table_id',
            'root_page_id' : 2,
            'unique' : True
        }
        self.system_indexes.insert()
        # PLEASE PLEASE PLEASE INDEX SYSTEM TABLES ON TABLE_ID
        
        # THEN INDEX SYSTEM_COLUMNS ON TABLE_ID
        # THEN INDEX SYSTEM INDEXES ON TABLE ID
        pass
            
    def _initialise_system_catalog_(self):
        self._initialise_system_tables_()
        self._initialise_system_columns_()
        
        self.load_catalog()
        
        self._initialise_system_indexes_()
        self._initialise_system_sequences_()
        
        self.page_allocator.page_manager.flush()
        
        self.load_catalog()
    
    

    def create_table(self, name, schema=None):
        if name in self.tables:
            return self.tables[name]
        
        page_id = self.page_allocator.allocate_page()
        table = Table(name, schema, page_id, self.page_allocator)
        self.tables[name] = table
        table_id = self._get_table_id_counter_()
        record = {'table_id': table_id, 'table_name': name, 'first_page_id': page_id}
        self.system_tables.insert(record)
        
        table_sys_columns = self._schema_to_sys_cols_(schema, table_id)
        
        for column in table_sys_columns:
            self.system_columns.insert(column)
        
        return table
    
    def insert(self, table_name: str, record: dict):
        if table_name not in self.tables:
            raise Exception('Table does not exist')
        
        return self.tables[table_name].insert(record)
        # location_info = self.tables[table_name].insert(record)
        # tables = self.system_tables.scan_all_records()
        # table = [x for x in tables if x['table_name'] == table_name][0]
        # id = table['table_id']
        
        # indexes = self.system_indexes.scan_all_records()
        # relevant_indexes = [x for x in indexes if x['table_id'] == id]
        
        # if len(relevant_indexes) > 0:
            # index = relevant_indexes[0]
            # 
            # bti = BTreeIndex(self.page_allocator, root_page_id=index['root_page_id'])
            # bti.insert(2, location_info)

    def get_table(self, name):
        return self.tables.get(name)
    
    def _load_schema_from_columns_(self, table_id):
        all_columns = self.system_columns.scan_all_records()
        relevant_columns = [x for x in all_columns if x['table_id'] == table_id]
        schema = Schema([])
        
        for column in relevant_columns:
            schema.fields.append((column['column_name'], column['data_type']))
        return schema

    def load_catalog(self, system_tables_pid = 2, system_columns_pid = 3):
        self.system_tables = Table(
            "system_tables", self._system_tables_schema_, system_tables_pid, self.page_allocator
        )
        self.system_columns = Table(
            "system_columns", self._system_columns_schema_, system_columns_pid, self.page_allocator
        )

        all_tables = self.system_tables.scan_all_records()
        
        for table_info in all_tables:
            tname = table_info['table_name']
            pid = table_info['first_page_id']
            schema = self._load_schema_from_columns_(table_info['table_id'])
            self.tables[tname] = Table(tname, schema, pid, self.page_allocator)
            
        if 'system_indexes' in self.tables:
            self.system_indexes = self.tables['system_indexes']
            
        if 'system_sequences' in self.tables:
            self.system_sequences = self.tables['system_sequences']
