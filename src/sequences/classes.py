from math import ceil

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
        increment,
        min_value,
        cycle,
        cache,
        manager
        ):
        
        self.id = id
        self.name = name
        self.current_value = current_value
        self.increment = increment
        self.min_value = min_value
        self.cache = cache
        self.cycle = cycle
        self._manager: SequenceManager = manager
        
        self.generate_values()
        
    def set_current_value(self, value):
        self.current_value = value
        self._manager._override_current_value(self.name, self._available_values[-1])
    
    def generate_values(self):
        print(f'generating sequence values ({self.name})')
            
        self._available_values = [(x * self.increment) for x in range(ceil(self.current_value / self.increment), ceil(self.current_value / self.increment) + self.cache)]
        self.set_current_value(self._available_values[-1])
        
    
    def nextval(self):
        if len(self._available_values) == 1:
            self.generate_values()
        
        return self._available_values.pop(0)            

class SequenceManager:
    def __init__(self, table):
        self._table = table
        
    def get_sequence(self, sequence_name: str):
        sequence = self._table.scan('name', sequence_name)
        return sequence
    
    def _get_sequence_id(self):
        sequence_id_sequence = self.get_sequence(SEQUENCE_ID_GENERATION_NAME)
        new_id = sequence_id_sequence['current_value'] + sequence_id_sequence['increment']
        self._override_current_value(SEQUENCE_ID_GENERATION_NAME, new_id)
        return new_id
        
    def _override_current_value(self, sequence_name, value):
        self._table.update(lambda rec: rec['name'] == sequence_name, {'current_value': value})
        
    def create_sequence(self, sequence_name, current_value = 0, increment = 1, min_value = 0, cache = 1, cycle = False):
        sequence_id = self._get_sequence_id()
        
        self._table.insert(
            {
                'sequence_id': sequence_id,
                'name': sequence_name,
                'current_value': current_value,
                'increment': increment,
                'min_value': min_value,
                'cache': cache,
                'cycle': str(cycle)
            }
        )
        
    def generate_sequence_object(self, sequence_name):
        sequence_data = self.get_sequence(sequence_name)
        
        return Sequence(
            id=sequence_data['sequence_id'],
            name=sequence_data['name'],
            current_value=sequence_data['current_value'],
            increment=sequence_data['increment'],
            min_value=sequence_data['min_value'],
            cycle=sequence_data['cycle'],
            cache=sequence_data['cache'],
            manager=self
        )