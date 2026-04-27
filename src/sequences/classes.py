from math import ceil
import os
import pathlib
from src.transactions.manager import get_transaction_manager

INTERNAL_SEQUENCE_TABLE_NAME = 'system_sequences'
SEQUENCE_ID_GENERATION_NAME = 'sys_S1'
SEQUENCE_NAME_GENERATION_NAME = 'sys_S2'
TABLE_ID_SEQUENCE_NAME = 'sys_S3'
INDEX_ID_SEQUENCE_NAME = 'sys_S4'

class Sequence:
    def __init__(
        self,
        id,
        name,
        current_value,
        start_value,
        increment,
        min_value,
        cycle,
        cache,
        manager
        ):
        
        self.id = id
        self.name = name
        self.current_value = current_value
        self.start_value = start_value
        self.increment = increment
        self.min_value = min_value
        self.cache = cache
        self.cycle = cycle
        self._manager: SequenceManager = manager
        
        self.generate_values()
        
    def set_current_value(self, value):
        self.current_value = value
        self._manager._write_sequence_file(self.name, self._available_values[-1])
    
    def generate_values(self):
        print(f'generating sequence values ({self.name})')
        
        start = ceil((self.current_value / self.increment) + 1)
        end = start + self.cache
        
        self._available_values = [
            (
                x * self.increment
            ) for x in range (
                start,
                end
            )
        ]
        self.set_current_value(self._available_values[-1])
        
    
    def nextval(self):
        if len(self._available_values) <= 1:
            self.generate_values()
        
        return self._available_values.pop(0)            

class SequenceManager:
    SEQUENCE_FILE_END = '.seq'
    
    def __init__(self, table):
        self._table = table
        self.sequence_base_location = pathlib.Path('internal/$sequences/')
        self.sequence_length = 64
        self.transaction_manager = get_transaction_manager()
        self.sequences = dict()

        self._initialise_()
        
    def _initialise_(self):
        
        if not self.sequence_base_location.exists():
            os.makedirs(self.sequence_base_location.absolute())
            
            print('Making Sequence folder')
            
            self._bootstrap_()
            
    def _bootstrap_(self):
        system_sequences_file = 'system_sequences' + self.SEQUENCE_FILE_END
        sys_seq_file_path = self.sequence_base_location / system_sequences_file
        
        if not sys_seq_file_path.exists():
            with open(sys_seq_file_path.absolute(), 'wb') as file:
                file.write(
                    int.to_bytes(
                        1,
                        self.sequence_length,
                        'little',
                        signed=False
                    )
                )
                
                file.flush()
                file.close()
                
    def _get_sequence_file_location_(self, name):
        return self.sequence_base_location / f'{name + self.SEQUENCE_FILE_END}'
                
    def _initialise_sequence_file_(self, sequence_name, start_value):
        sequence_loc: pathlib.Path = self._get_sequence_file_location_(sequence_name)
        
        if not sequence_loc.exists():
            with open(sequence_loc, 'wb') as file:
                file.write(
                    int.to_bytes(
                        start_value,
                        self.sequence_length,
                        'little',
                        signed=False
                    )
                ) # write start value
                
                file.flush()
                file.close()

    def _load_sequence_file(self, sequence_config):
        self._initialise_sequence_file_(sequence_config['name'], sequence_config['start_value'])
        
        sequence_location = self._get_sequence_file_location_(sequence_config['name'])
        
        with open(sequence_location, mode='rb') as file:
            content = file.read()
            file.close()
        
        return int.from_bytes(content, 'little', signed=False)
            
    def get_sequence(self, sequence_name: str, tx_snapshot):
        
        if sequence_name not in self.sequences:
            sequence_config = self._table.scan(lambda x: x['name'] == sequence_name, tx_snapshot)
            current_value = self._load_sequence_file(sequence_config)
            sequence = Sequence(
                sequence_config['sequence_id'],
                sequence_config['name'],
                current_value,
                sequence_config['start_value'],
                sequence_config['increment'],
                sequence_config['min_value'],
                sequence_config['cycle'],
                sequence_config['cache'],
                self
            )
            self.sequences[sequence_config['name']] = sequence
            return sequence
        
        return self.sequences[sequence_name]
    
    def _get_sequence_id(self, tx_id, tx_snapshot):
        sequence_id_sequence = self.get_sequence(SEQUENCE_ID_GENERATION_NAME, tx_snapshot)
        new_id = sequence_id_sequence.nextval()
        return new_id
        
    def _write_sequence_file(self, sequence_name, value):
        sequence_location = self._get_sequence_file_location_(sequence_name)
        
        with open(sequence_location, 'wb+') as file:
            file.write(
                int.to_bytes(
                    value,
                    self.sequence_length,
                    'little',
                    signed=False
                )
            ) # write start value
            
            file.flush()
            file.close()
        
    def create_sequence(self, sequence_name, start_value = 0, increment = 1, min_value = 0, cache = 1, cycle = False):
        
        tx = self.transaction_manager.get_new_transaction()
        
        sequence_id = self._get_sequence_id(tx.id, tx.start_snapshot)
        
        self._table.insert(
            {
                'sequence_id': sequence_id,
                'name': sequence_name,
                'start_value': start_value,
                'increment': increment,
                'min_value': min_value,
                'cache': cache,
                'cycle': str(cycle)
            },
            tx
        )
        
        tx.commit()
