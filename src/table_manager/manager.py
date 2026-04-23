from .table import Table
from src.config import ENDIAN_TYPE
from src.records.structured_records import Schema, StructuredDataRecordPage
from src.datatypes.classes import DataType, get_datatype
from src.pages.allocator import PageAllocator
from src.catalog.header import CatalogHeader
from src.sequences.classes import INTERNAL_SEQUENCE_TABLE_NAME, SEQUENCE_ID_GENERATION_NAME, SEQUENCE_NAME_GENERATION_NAME, TABLE_ID_SEQUENCE_NAME, INDEX_ID_SEQUENCE_NAME, SequenceManager
from src.indices.btree2 import BTreeIndex
from src.table_manager.seed.core.internal_system.system_tables import SystemTablesSeedData
from src.table_manager.seed.core.internal_system.system_columns import SystemColumnsSeedData
from src.table_manager.schemas.core.internal_system import (
    SYSTEM_TABLES_SCHEMA,
    SYSTEM_COLUMNS_SCHEMA,
    SYSTEM_INDEXES_SCHEMA,
    SYSTEM_SEQUENCES_SCHEMA
)
from src.table_manager.seed.core.internal_system.system_sequences import SystemSequencesSeedData

class TableManager:
    def _schema_to_sys_cols_(self, schema, table_id):
        columns = []
        table_name = self.system_tables.scan(lambda x: x['table_id'] == table_id)['table_name']
        
        for i, field_data in enumerate(schema.fields):
            columns.append(
                {
                    'table_id': table_id,
                    'column_id': i+1,
                    'column_name': field_data[0],
                    'table_name': table_name,
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
        system_tables_page = StructuredDataRecordPage(self.page_allocator.get_page(2).data) # initial page
        
        system_tables_page.insert_record(
            schema = SYSTEM_TABLES_SCHEMA,
            record = SystemTablesSeedData.SYSTEM_TABLES_SYSTEM_TABLES_RECORD
        ) # insert tables record to table page (self-reference / bootstrap)
        
        system_tables_page.insert_record(
            schema = SYSTEM_TABLES_SCHEMA,
            record = SystemTablesSeedData.SYSTEM_TABLES_SYSTEM_COLUMNS_RECORD
        ) # same w columns
        
        self.page_allocator.page_manager.write_page(2, system_tables_page.data) # force write as we're still booting up
        self.page_allocator._mark_page_id(2)
        
    def _initialise_system_indexes_(self):
        self.system_indexes = self.create_table('system_indexes', SYSTEM_INDEXES_SCHEMA) # Background works mostly there for the catalog
    
    def _initialise_system_sequences_(self):
        self.system_sequences = self.create_table(INTERNAL_SEQUENCE_TABLE_NAME, SYSTEM_SEQUENCES_SCHEMA)
        
        self.system_sequences.insert(SystemSequencesSeedData.SYSTEM_SEQUENCES_SYSTEM_SEQUENCES_RECORD)
        
        self.sequence_manager = SequenceManager(self.system_sequences)
        self.sequence_manager.create_sequence(SEQUENCE_NAME_GENERATION_NAME, 3, 1, 1, 1, 'False')
        self.sequence_manager.create_sequence(TABLE_ID_SEQUENCE_NAME, 9, 1, 1, 1, 'False')
        self.sequence_manager.create_sequence(INDEX_ID_SEQUENCE_NAME, 4, 1, 1, 10, 'False')
        
    def _initialise_system_columns_(self):
        system_columns_page = StructuredDataRecordPage(self.page_allocator.get_page(3).data)
        
        systabs_records = []
        syscols_records = []
        
        for rec in SystemColumnsSeedData.SYSTEM_COLUMNS_SYSTEM_TABLES_RECORD:
            slot = system_columns_page.insert_record(
                schema = SYSTEM_COLUMNS_SCHEMA,
                record = rec
            )
            systabs_records.append((1, (3, slot)))

        for rec in SystemColumnsSeedData.SYSTEM_COLUMNS_SYSTEM_COLUMNS_RECORD:
            slot = system_columns_page.insert_record(
                schema = SYSTEM_COLUMNS_SCHEMA,
                record = rec
            )
            syscols_records.append((2, (3, slot)))
            
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
            'table_name': 'system_tables',
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
            'table_name': 'system_columns',
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
            'table_name': 'system_indexes',
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
        
        self.create_index('idx_system_tables_name', 'system_tables', 'table_name', unique = True)
        
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
        relevant_columns = self.system_columns.scan(lambda x: x['table_id'] == table_id)
        schema = Schema([])
        
        for column in relevant_columns:
            schema.fields.append((column['column_name'], column['data_type'], column['data_length']))
        return schema
    
    def get_index_by_table_col(self, table_name, column_name):
        table_indexes = self.system_indexes.scan(lambda x: x['column_name'] == column_name and x['table_name'] == table_name)
        
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
        existing_index = self.system_indexes.scan(lambda x: x['table_name'] == table_name and x['column_name'] == column_name)
        
        if len(existing_index) > 0:
            return existing_index
        
        # table = self.system_tables.scan(lambda x: x['table_name'] == table_name)
        # indexes = self.system_indexes.scan(lambda x: x['table_id'] == table['table_id'])
        
        # index_ids = [i['index_id'] for i in indexes] + [-1]
        # new_index_id = max(index_ids) + 1
        
        if hasattr(self, 'seqeuence_manager'):
            index_id_sequence = self.sequence_manager.generate_sequence_object(INDEX_ID_SEQUENCE_NAME)
            index_id = index_id_sequence.nextval()
        else:
            indexes = self.system_indexes.scan(lambda x: x['table_name'] == table_name)
            
            if isinstance(indexes, dict):
                new_index_id = indexes['index_id'] + 1
            elif isinstance(indexes, list):    
                index_ids = [i['index_id'] for i in indexes] + [-1]
                new_index_id = max(index_ids) + 1
        
        # column_records = self.system_columns.scan_all_records()
        
        # column = None
        
        # for o in column_records:
        #     if o['column_name'] == column_name and o['table_id'] == table['table_id']:
        #         column = o
        #         break

        column = self.system_columns.scan(lambda x: x['table_name'] == table_name and x['column_name'] == column_name)
        table = self.system_tables.scan(lambda x: x['table_name'] == table_name)
        
        if len(column) == 0 or len(table) == 0:
            raise Exception('No column / table found')
        
        d_type_class = get_datatype(column['data_type'])
        datatype = d_type_class(length=column['data_length'], signed=False)
        bti = BTreeIndex(self.page_allocator, datatype=datatype)
        
        eeee = {
            'index_id': new_index_id,
            'name': index_name,
            'table_id': table['table_id'],
            'table_name': table_name,
            'column_name': column_name,
            'root_page_id': bti.root_page_id,
            'unique': str(unique)
        }
        
        all_records, all_rids = self.tables[table_name].scan_all_records(include_rids=True)
        filtered_records = [x[column_name] for x in all_records]
        
        for (record, rid) in zip(filtered_records, all_rids):
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
            "system_tables", SYSTEM_TABLES_SCHEMA, system_tables_pid, self.page_allocator
        )
        
        if 'system_columns' not in self.tables:
            self.tables['system_columns'] = Table(
            "system_columns", SYSTEM_COLUMNS_SCHEMA, system_columns_pid, self.page_allocator
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