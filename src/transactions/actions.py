import enum
import logging
from typing import Dict, Optional, Any

txa_logger = logging.getLogger('TransactionAction')
logging.basicConfig(level=logging.info)

class TXActionType(enum.Enum):
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    INDEX_INSERT = 'IDX_INSERT'
    INDEX_DELETE = 'IDX_DELETE'

class TXAction:
    action_type: TXActionType
    table: Any
    rid: tuple[int, int]
    old_data: Optional[Dict]
    index: Optional[Any]
    key: Optional[Any]
    
    def __repr__(self):
        return f'TX-{self.action_type.value}({self.table}, {self.rid}, {self.old_data})'
    
    def __init__(
        self,
        action_type: TXActionType,
        table,
        rid: tuple[int,int],
        old_data: Optional[Dict] = None,
        new_data: Optional[Dict] = None,
        index = None,
        key = None
        ):
        
        self.action_type = action_type
        self.table = table
        self.rid = rid
        self.old_data = old_data
        self.new_data = new_data
        self.index = index
        self.key = key