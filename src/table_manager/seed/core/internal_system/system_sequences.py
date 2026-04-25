from src.table_manager.seed.core.utils import BaseSeedData
from src.sequences.classes import (
    SEQUENCE_ID_GENERATION_NAME,
    INDEX_ID_SEQUENCE_NAME
)

class SystemSequencesSeedData(BaseSeedData):
    SYSTEM_SEQUENCES_SYSTEM_SEQUENCES_RECORD = {
        'sequence_id': 1,
        'name': SEQUENCE_ID_GENERATION_NAME,
        'start_value': 1,
        'increment': 1,
        'min_value': 1,
        'cache': 1,
        'cycle': 'False'
    }
    
    SYSTEM_SEQUENCES_INDEX_ID_RECORD = {
        'sequence_id': 2,
        'name': INDEX_ID_SEQUENCE_NAME,
        'start_value': 4,
        'increment': 1,
        'min_value': 1,
        'cache': 10,
        'cycle': 'False'
    }