from .table import Table
from src.config import ENDIAN_TYPE
from src.records.structured_records import Schema, StructuredDataRecordPage
from src.datatypes.classes import DataType, get_datatype
from src.pages.allocator import PageAllocator
from src.catalog.header import CatalogHeader
from src.sequences.classes import INTERNAL_SEQUENCE_TABLE_NAME, SEQUENCE_ID_GENERATION_NAME, SEQUENCE_NAME_GENERATION_NAME, TABLE_ID_SEQUENCE_NAME, SequenceManager
# from src.indices.classes import BTreeIndex
from src.indices.btree2 import BTreeIndex

class TableManager:
    @property
    def _system_tables_schema_(self):
        return Schema([
            ('table_id', DataType.INTEGER, 30),
            ('table_name', DataType.STRING, 30),
            ('first_page_id', DataType.INTEGER, 30),
        ])
  
    @property
    def _system_columns_schema_(self):      
        return Schema([
            ('table_id', DataType.INTEGER, 30),
            ('column_name', DataType.STRING, 30),
            ('data_type', DataType.STRING, 30),
            ('data_length', DataType.INTEGER, 30),
            ('ordinal_position', DataType.INTEGER, 30),
        ])
        
    @property
    def _system_indexes_schema_(self):
        return Schema([
            ('index_id', DataType.INTEGER, 30),
            ('name', DataType.STRING, 30),
            ('table_id', DataType.INTEGER, 30),
            ('column_name', DataType.STRING, 30),
            ('root_page_id', DataType.INTEGER, 30),
            ('unique', DataType.BOOLEAN, 1)
        ])
        
    @property
    def _system_sequences_schema_(self):
        return Schema([
            ('sequence_id', DataType.INTEGER, 30),
            ('name', DataType.STRING, 30),
            ('current_value', DataType.INTEGER, 30),
            ('increment', DataType.INTEGER, 30),
            ('min_value', DataType.INTEGER, 30),
            ('cache', DataType.INTEGER, 30),
            ('cycle', DataType.BOOLEAN, 1)
        ])
        
    def _schema_to_sys_cols_(self, schema, table_id):
        columns = []
        
        for i, field_data in enumerate(schema.fields):
            columns.append(
                {
                    'table_id': table_id,
                    'column_name': field_data[0],
                    'data_type': field_data[1],
                    'data_length': field_data[2],
                    'ordinal_position': i + 1
                }
            )
            
        return columns
    
    def _get_table_id_counter_(self):
        if not hasattr(self, 'sequence_manager'):
            self.catalog_header.table_counter += 1
            return self.catalog_header.table_counter
        else:
            seq = self.sequence_manager.generate_sequence_object(TABLE_ID_SEQUENCE_NAME)
            return seq.nextval()
    
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
        
        sys_t_slot_ptr = system_tables_page.insert_record(
            schema = self._system_tables_schema_,
            record=system_tables_sys_table_record
        )
        
        sys_c_slot_ptr = system_tables_page.insert_record(
            schema=self._system_tables_schema_,
            record= system_columns_sys_table_record
        )
        
        self.page_allocator.page_manager.write_page(2, system_tables_page.data)
        self.page_allocator._mark_page_id(2)
        
    def _initialise_system_indexes_(self):
        self.system_indexes = self.create_table('system_indexes', self._system_indexes_schema_)
    
    def _initialise_system_sequences_(self):
        self.system_sequences = self.create_table(INTERNAL_SEQUENCE_TABLE_NAME, self._system_sequences_schema_)
        
        sequence_id_sequence = {
            'sequence_id': 1,
            'name': SEQUENCE_ID_GENERATION_NAME,
            'current_value': 1,
            'increment': 1,
            'min_value': 1,
            'cache': 1,
            'cycle': 'False'
        }
        
        self.system_sequences.insert(sequence_id_sequence)
        
        self.sequence_manager = SequenceManager(self.system_sequences)
        self.sequence_manager.create_sequence(SEQUENCE_NAME_GENERATION_NAME, 3, 1, 1, 1, 'False')
        self.sequence_manager.create_sequence(TABLE_ID_SEQUENCE_NAME, 6, 1, 1, 1, 'False')
        
    def table_name_to_id(self, table_name: str):
        if table_name in self.tables:
            records = self.system_tables.scan_all_records()
            records = [x for x in records if x['table_name'] == table_name]
            return records[0]['table_id']
        
    def _initialise_system_columns_(self):
        system_columns_page = StructuredDataRecordPage(self.page_allocator.get_page(3).data)
        
        system_tables_sys_columns_records = [
            {
                'table_id': 1,
                'column_id': 1,
                'column_name': 'table_id',
                'data_type': DataType.INTEGER,
                'data_length': 30,
                'ordinal_position': 1
            },
            {
                'table_id': 1,
                'column_id': 2,
                'column_name': 'table_name',
                'data_type': DataType.STRING,
                'data_length': 30,
                'ordinal_position': 2
            },
            {
                'table_id': 1,
                'column_id': 1,
                'column_name': 'first_page_id',
                'data_type': DataType.INTEGER,
                'data_length': 30,
                'ordinal_position': 3
            }
        ]

        system_columns_sys_columns_records = [
            {
                'table_id': 2,
                'column_id': 1,
                'column_name': 'table_id',
                'data_type': DataType.INTEGER,
                'data_length': 30,
                'ordinal_position': 1
            },
            {
                'table_id': 2,
                'column_id': 2,
                'column_name': 'column_name',
                'data_type': DataType.STRING,
                'data_length': 30,
                'ordinal_position': 2
            },
            {
                'table_id': 2,
                'column_id': 3,
                'column_name': 'data_type',
                'data_type': DataType.STRING,
                'data_length': 30,
                'ordinal_position': 3
            },
            {
                'table_id': 2,
                'column_id': 4,
                'column_name': 'data_length',
                'data_type': DataType.INTEGER,
                'data_length': 30,
                'ordinal_position': 3
            },
            {
                'table_id': 2,
                'column_id': 5,
                'column_name': 'ordinal_position',
                'data_type': DataType.INTEGER,
                'data_length': 30,
                'ordinal_position': 4
            }
        ]

        systabs_records = []
        syscols_records = []
        for i in system_tables_sys_columns_records:
            b = system_columns_page.insert_record(
                schema=self._system_columns_schema_,
                record = i
            )
            systabs_records.append((1, (3, b)))
            


        for i in system_columns_sys_columns_records:
            a = system_columns_page.insert_record(
                schema=self._system_columns_schema_,
                record = i
            )
            syscols_records.append((2, (3,a)))
            
        self.page_allocator.page_manager.write_page(3, system_columns_page.data)
        self.page_allocator._mark_page_id(3)
    
    def _initialise_metadata_indexes_(self):
        d_type_class = get_datatype('I')
        datatype = d_type_class(length=30, signed=False)
        
        system_table_index = BTreeIndex(
            self.page_allocator,
            datatype=datatype
        )
        
        system_table_table_id_index = {
            'index_id' : 1,
            'name' : 'pk_system_tables_table_id',
            'table_id' : 1,
            'column_name' : 'table_id',
            'root_page_id' : system_table_index.root_page_id,
            'unique' : True
        }
        self.system_indexes.insert(system_table_table_id_index)
        
        system_table_index.insert(1, (2,0)) # SYSTEM TABLE IDX, followed by page ptr, slot ptr
        system_table_index.insert(2, (2,1)) # SYSTEM COLS IDX, followed by page ptr, slot ptr
        system_table_index.insert(6, (2,2)) # SYSTEM INDEX IDX, followed by page ptr, slot ptr
        
        system_columns_index = BTreeIndex(
            self.page_allocator,
            datatype=datatype
        )
        
        system_column_table_id_index = {
            'index_id' : 2,
            'name' : 'idx_sys_col_table_id',
            'table_id' : 2,
            'column_name' : 'table_id',
            'root_page_id' : system_columns_index.root_page_id,
            'unique' : True
        }
        
        self.system_indexes.insert(system_column_table_id_index)
        
        system_columns_index.insert(1, (3, 0))
        system_columns_index.insert(1, (3, 1))
        system_columns_index.insert(1, (3, 2))
        
        system_columns_index.insert(2, (3, 3))
        system_columns_index.insert(2, (3, 4))
        system_columns_index.insert(2, (3, 5))
        system_columns_index.insert(2, (3, 6))
        system_columns_index.insert(2, (3, 7))
        
        system_columns_index.insert(6, (3, 8))
        system_columns_index.insert(6, (3, 9))
        system_columns_index.insert(6, (3, 10))
        system_columns_index.insert(6, (3, 11))
        system_columns_index.insert(6, (3, 12))
        system_columns_index.insert(6, (3, 13))
        
        system_indexes_index = BTreeIndex(
            self.page_allocator,
            datatype=datatype
        )
        
        system_indexes_table_id_index = {
            'index_id' : 3,
            'name' : 'idx_sys_ind_table_idx',
            'table_id' : 6,
            'column_name' : 'table_id',
            'root_page_id' : system_indexes_index.root_page_id,
            'unique' : True
        }
        self.system_indexes.insert(system_indexes_table_id_index)
        system_indexes_index.insert(1, (4, 0))
        system_indexes_index.insert(2, (4, 1))
        system_indexes_index.insert(6, (4, 2))
        
        self.system_tables.indexes['table_id'] = system_table_index
        self.system_columns.indexes['table_id'] = system_columns_index
        self.system_indexes.indexes['table_id'] = system_indexes_index
        
        pass
            
    def _initialise_system_catalog_(self):
        self._initialise_system_tables_()
        self._initialise_system_columns_()
        
        self.load_catalog()
        
        self._initialise_system_indexes_()
        self._initialise_metadata_indexes_()
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

    def get_table(self, name):
        return self.tables.get(name)
    
    def _load_schema_from_columns_(self, table_id):
        all_columns = self.system_columns.scan_all_records()
        relevant_columns = [x for x in all_columns if x['table_id'] == table_id]
        schema = Schema([])
        
        for column in relevant_columns:
            schema.fields.append((column['column_name'], column['data_type'], column['data_length']))
        return schema
    
    def get_index_by_table_col(self, table_name, column_name):
        if table_name in self.tables:
            table_record = self.system_tables.scan('table_name', table_name)
                    
            if table_record is not None:
                table_indexes = self.system_indexes.scan('column_name', column_name)
                
                if table_indexes == []:
                    return None
                
                return table_indexes
    
    def table_column_has_index(self, table_name, column_name):
        index = self.get_index_by_table_col(table_name, column_name)
        
        if index is not None:
            return True
        
        return False
    
    # def new_index(self, index_name, table_name, column_name, unique = False):
        # existing_index
    
    def create_index(self, index_name, table_name, column_name, unique = False):
        existing_index = self.get_index_by_table_col(table_name, column_name)
        if existing_index is not None:
            return existing_index
        
        table = self.system_tables.scan('table_name', table_name)
        indexes = self.system_indexes.scan('table_id', table['table_id'])
        
        index_ids = [i['index_id'] for i in indexes] + [-1]
        new_index_id = max(index_ids) + 1
        
        column_records = self.system_columns.scan_all_records()
        
        column = None
        
        for o in column_records:
            if o['column_name'] == column_name and o['table_id'] == table['table_id']:
                column = o
                break
        
        if column is not None:
            d_type_class = get_datatype(column['data_type'])
            datatype = d_type_class(length=column['data_length'], signed=False)
            bti = BTreeIndex(self.page_allocator, datatype=datatype) ## this seems to fuck up page 92
            
            eeee = {
                'index_id': new_index_id,
                'name': index_name,
                'table_id': table['table_id'],
                'column_name': column_name,
                'root_page_id': bti.root_page_id,
                'unique': str(unique)
            }
            
            all_records, all_rids = self.tables[table_name].scan_all_records(include_rids=True)
            filtered_records = [x[column_name] for x in all_records]
            
            for (record, rid) in zip(filtered_records, all_rids): # somewhere in here it condenses down the array so it fails the page size check
                bti.insert(record, rid)
                
            self.system_indexes.insert(eeee)
            self.tables[table_name].indexes[column_name] = bti
            
            
    def load_indexes(self):
        all_indexes = self.system_indexes.scan_all_records()
        
        for idx in all_indexes:
            self
            
    @property
    def system_tables(self):
        return self.tables['system_tables']
    
    @property
    def system_columns(self):
        return self.tables['system_columns']

    def load_catalog(self, system_tables_pid = 2, system_columns_pid = 3):
        
        
        swap_indexes = dict()
        
        for tname in self.tables:
            swap_indexes[tname] = self.tables[tname].indexes
        
        if 'system_table' not in self.tables:
            self.tables['system_tables'] = Table(
            "system_tables", self._system_tables_schema_, system_tables_pid, self.page_allocator
        )
        
        if 'system_columns' not in self.tables:
            self.tables['system_columns'] = Table(
            "system_columns", self._system_columns_schema_, system_columns_pid, self.page_allocator
        )

        all_tables = self.system_tables.scan_all_records()
        
        for table_info in all_tables:
            tname = table_info['table_name']
            pid = table_info['first_page_id']
            schema = self._load_schema_from_columns_(table_info['table_id'])
            self.tables[tname] = Table(tname, schema, pid, self.page_allocator)
            
        for tname, indexes in swap_indexes.items():
            self.tables[tname].indexes = indexes
            
        if 'system_indexes' in self.tables:
            self.system_indexes = self.tables['system_indexes']
            self.load_indexes()
            
        if INTERNAL_SEQUENCE_TABLE_NAME in self.tables:
            self.system_sequences = self.tables[INTERNAL_SEQUENCE_TABLE_NAME]
            self.sequence_manager = SequenceManager(self.system_sequences)